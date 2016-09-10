import unittest
import zmq
from hedgehog.utils import discovery
from hedgehog.utils.discovery.service_node import ServiceNode, endpoint_to_port


class DiscoveryTests(unittest.TestCase):
    def test_service_request_message(self):
        old = discovery.Request('test')
        new = discovery.Msg.parse(discovery.Msg.serialize(old))
        self.assertEqual(new, old)

    def test_service_update_message(self):
        old = discovery.Update(ports=[1])
        new = discovery.Msg.parse(discovery.Msg.serialize(old))
        self.assertEqual(new, old)

    def test_endpoint_to_port(self):
        port = endpoint_to_port(b'tcp://127.0.0.1:5555')
        self.assertEqual(port, 5555)

    def test_discovery(self):
        ctx = zmq.Context.instance()
        with ServiceNode("Node 1", ctx) as node1, \
                ServiceNode("Node 2", ctx) as node2:

            msg = discovery.ApiMsg.parse(node1.events.recv())
            self.assertIsInstance(msg, discovery.Enter)
            self.assertEqual(msg.name, 'Node 2')

            endpoint = msg.address.rsplit(':', 1)[0] + ':5555'

            msg = discovery.ApiMsg.parse(node2.events.recv())
            self.assertIsInstance(msg, discovery.Enter)
            self.assertEqual(msg.name, 'Node 1')

            node2.join('hedgehog_server')
            node1.join('hedgehog_server')

            msg = discovery.ApiMsg.parse(node1.events.recv())
            self.assertIsInstance(msg, discovery.Join)
            self.assertEqual(msg.name, 'Node 2')
            self.assertEqual(msg.group, 'hedgehog_server')

            msg = discovery.ApiMsg.parse(node2.events.recv())
            self.assertIsInstance(msg, discovery.Join)
            self.assertEqual(msg.name, 'Node 1')
            self.assertEqual(msg.group, 'hedgehog_server')

            node2.add_service('hedgehog_server', 5555)

            msg = discovery.ApiMsg.parse(node1.events.recv())
            self.assertIsInstance(msg, discovery.Shout)
            self.assertEqual(msg.name, 'Node 2')
            self.assertEqual(msg.group, 'hedgehog_server')
            self.assertEqual(msg.payload, discovery.Msg.serialize(discovery.Update(ports=[5555])))

            node1.request_service('hedgehog_server')

            msg = discovery.ApiMsg.parse(node2.events.recv())
            self.assertIsInstance(msg, discovery.Shout)
            self.assertEqual(msg.name, 'Node 1')
            self.assertEqual(msg.group, 'hedgehog_server')
            self.assertEqual(msg.payload, discovery.Msg.serialize(discovery.Request()))

            node2.peers[msg.uuid].update_service('hedgehog_server')

            msg = discovery.ApiMsg.parse(node1.events.recv())
            self.assertIsInstance(msg, discovery.Whisper)
            self.assertEqual(msg.name, 'Node 2')
            self.assertEqual(msg.payload, discovery.Msg.serialize(discovery.Update('hedgehog_server', [5555])))

            node1.get_endpoints('hedgehog_server')

            msg = discovery.ApiMsg.parse(node1.events.recv())
            self.assertIsInstance(msg, discovery.Service)
            self.assertEqual(msg.service, 'hedgehog_server')
            self.assertEqual(msg.endpoints, {endpoint})

    def test_pyre(self):
        from uuid import UUID
        from pyre.pyre import Pyre
        from hedgehog.utils.zmq.poller import Poller

        ctx = zmq.Context()
        poller = Poller()

        def handle_pipe(node):
            msg = node.actor.pipe.recv_multipart()
            print(node.name(), "pipe: ", msg)

        def handle_inbox(node):
            msg = node.inbox.recv_multipart()
            print(node.name(), "inbox:", msg)

            cmd, id, name, *args = msg
            if cmd == b'JOIN':
                node.whisper(UUID(bytes=id), [b'a', b'b'])
                node.shout('test', [b'a', b'b'])

        def mknode(i):
            node = Pyre("Node {}".format(i), ctx)
            node.join('test')
            poller.register(node.actor.pipe, zmq.POLLIN, lambda n=node: handle_pipe(n))
            poller.register(node.inbox, zmq.POLLIN, lambda n=node: handle_inbox(n))
            node.start()
            return node

        nodes = [mknode(i) for i in range(2)]

        count = 0
        while count < 8:
            for _, _, handler in poller.poll():
                handler()
                count += 1

        for node in nodes:
            node.stop()

    def test_node(self):
        from uuid import UUID
        from hedgehog.utils.discovery.node import Node
        from hedgehog.utils.zmq.poller import Poller

        ctx = zmq.Context()
        poller = Poller()

        def handle_evt(node):
            msg = node.actor.evt_pipe.recv_multipart()
            print(node._name, msg)

            cmd, id, name, *args = msg
            if cmd == b'JOIN':
                node.whisper(UUID(bytes=id), [b'a', b'b'])
                node.shout('test', [b'a', b'b'])

        def mknode(i):
            node = Node(ctx, "Node {}".format(i))
            node.start()
            node.join('test')
            poller.register(node.actor.evt_pipe, zmq.POLLIN, lambda n=node: handle_evt(n))
            return node

        nodes = [mknode(i) for i in range(2)]

        count = 0
        while count < 8:
            for _, _, handler in poller.poll():
                handler()
                count += 1

        for node in nodes:
            node.stop()

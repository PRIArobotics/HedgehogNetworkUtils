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
        from pyre.pyre import Pyre

        ctx = zmq.Context()

        node1, node2 = nodes = [Pyre("Node {}".format(i), ctx) for i in range(2)]

        def other(node):
            return node1 if node is node2 else node2

        for node in nodes:
            node.start()

        try:
            for node in nodes:
                command, uuid, name, headers, endpoint = node.recv()
                self.assertEqual(command.decode(), "ENTER")
                self.assertEqual(uuid, other(node).uuid().bytes)
                self.assertEqual(name.decode(), other(node)._name)

            for node in nodes:
                node.join('test')

            for node in nodes:
                command, uuid, name, group = node.recv()
                self.assertEqual(command.decode(), "JOIN")
                self.assertEqual(uuid, other(node).uuid().bytes)
                self.assertEqual(name.decode(), other(node).name())
                self.assertEqual(group.decode(), 'test')

            node1.whisper(node2.uuid(), [b'a', b'b'])
            command, uuid, name, *msg = node2.recv()

            self.assertEqual(command.decode(), "WHISPER")
            self.assertEqual(uuid, node1.uuid().bytes)
            self.assertEqual(name.decode(), node1.name())
            self.assertEqual(msg, [b'a'])
            # self.assertEqual(msg, [b'a', b'b'])  # TODO

            node1.shout('test', [b'a', b'b'])
            command, uuid, name, group, *msg = node2.recv()

            self.assertEqual(command.decode(), "SHOUT")
            self.assertEqual(uuid, node1.uuid().bytes)
            self.assertEqual(name.decode(), node1.name())
            self.assertEqual(group.decode(), 'test')
            self.assertEqual(msg, [b'a'])
            # self.assertEqual(msg, [b'a', b'b'])  # TODO
        finally:
            for node in nodes:
                node.stop()

    def test_node(self):
        from uuid import UUID
        from hedgehog.utils.discovery.node import Node

        ctx = zmq.Context()

        node1, node2 = nodes = [Node(ctx, "Node {}".format(i)) for i in range(2)]

        def other(node):
            return node1 if node is node2 else node2

        with node1, node2:
            for node in nodes:
                command, uuid, name, headers, endpoint = node.evt_pipe.recv_multipart()
                self.assertEqual(command, b'ENTER')
                self.assertEqual(name.decode(), other(node)._name)
                other(node)._uuid = uuid  # TODO

            for node in nodes:
                node.join('test')

            for node in nodes:
                command, uuid, name, group = node.evt_pipe.recv_multipart()
                self.assertEqual(command, b'JOIN')
                self.assertEqual(uuid, other(node)._uuid)
                self.assertEqual(name.decode(), other(node)._name)
                self.assertEqual(group.decode(), 'test')

            node1.whisper(UUID(bytes=node2._uuid), [b'a', b'b'])
            command, uuid, name, *msg = node2.evt_pipe.recv_multipart()

            self.assertEqual(command, b'WHISPER')
            self.assertEqual(uuid, node1._uuid)
            self.assertEqual(name.decode(), node1._name)
            self.assertEqual(msg, [b'a'])
            # self.assertEqual(msg, [b'a', b'b'])  # TODO

            node1.shout('test', [b'a', b'b'])
            command, uuid, name, group, *msg = node2.evt_pipe.recv_multipart()

            self.assertEqual(command, b'SHOUT')
            self.assertEqual(uuid, node1._uuid)
            self.assertEqual(name.decode(), node1._name)
            self.assertEqual(group.decode(), 'test')
            self.assertEqual(msg, [b'a'])
            # self.assertEqual(msg, [b'a', b'b'])  # TODO

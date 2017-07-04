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

    def test_service_node(self):
        ctx = zmq.Context.instance()

        SERVICE = 'hedgehog_server'
        node1, node2 = nodes = [ServiceNode(ctx, "Node {}".format(i)) for i in range(2)]

        def other(node):
            return node1 if node is node2 else node2

        with node1, node2:
            # check ENTER

            for node in nodes:
                command, uuid, name, headers, endpoint = node.evt_pipe.recv_multipart()
                self.assertEqual(command, b'ENTER')
                self.assertEqual(uuid, other(node).uuid.bytes)
                self.assertEqual(name.decode(), other(node).name)
                other(node)._endpoint = endpoint.decode()  # TODO

            # check JOIN

            for node in nodes:
                node.join(SERVICE)

            for node in nodes:
                command, uuid, name, group = node.evt_pipe.recv_multipart()
                self.assertEqual(command, b'JOIN')
                self.assertEqual(uuid, other(node).uuid.bytes)
                self.assertEqual(name.decode(), other(node).name)
                self.assertEqual(group.decode(), SERVICE)

            # check add_service

            endpoint = node1._endpoint.rsplit(':', 1)[0] + ':5555'
            node1.add_service(SERVICE, 5555)

            command, = node2.evt_pipe.recv_multipart()
            self.assertEqual(command, b'UPDATE')
            peer = node2.evt_pipe.pop()
            self.assertEqual(peer.name, node1.name)
            self.assertEqual(peer.uuid, node1.uuid.bytes)
            self.assertEqual(peer.services, {SERVICE: {endpoint}})

            # check get_peers

            self.assertEqual(
                {peer.name: (peer.uuid, peer.services) for peer in node2.get_peers()},
                {peer.name: (peer.uuid.bytes, services) for peer, services in [(node1, {SERVICE: {endpoint}})]})

            # check request_service

            node2.request_service(SERVICE)

            command, = node2.evt_pipe.recv_multipart()
            self.assertEqual(command, b'UPDATE')
            peer = node2.evt_pipe.pop()
            self.assertEqual(peer.name, node1.name)
            self.assertEqual(peer.uuid, node1.uuid.bytes)
            self.assertEqual(peer.services, {SERVICE: {endpoint}})

            # check remove_service

            node1.remove_service(SERVICE, 5555)

            command, = node2.evt_pipe.recv_multipart()
            self.assertEqual(command, b'UPDATE')
            peer = node2.evt_pipe.pop()
            self.assertEqual(peer.name, node1.name)
            self.assertEqual(peer.uuid, node1.uuid.bytes)
            self.assertEqual(peer.services, {})

            # check get_peers

            self.assertEqual(
                {peer.name: (peer.uuid, peer.services) for peer in node2.get_peers()},
                {peer.name: (peer.uuid.bytes, services) for peer, services in [(node1, {})]})

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
            self.assertEqual(msg, [b'a', b'b'])

            node1.shout('test', [b'a', b'b'])
            command, uuid, name, group, *msg = node2.recv()

            self.assertEqual(command.decode(), "SHOUT")
            self.assertEqual(uuid, node1.uuid().bytes)
            self.assertEqual(name.decode(), node1.name())
            self.assertEqual(group.decode(), 'test')
            self.assertEqual(msg, [b'a', b'b'])
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
                self.assertEqual(uuid, other(node).uuid.bytes)
                self.assertEqual(name.decode(), other(node).name)

            for node in nodes:
                node.join('test')

            for node in nodes:
                command, uuid, name, group = node.evt_pipe.recv_multipart()
                self.assertEqual(command, b'JOIN')
                self.assertEqual(uuid, other(node).uuid.bytes)
                self.assertEqual(name.decode(), other(node).name)
                self.assertEqual(group.decode(), 'test')

            node1.whisper(node2.uuid, [b'a', b'b'])
            command, uuid, name, *msg = node2.evt_pipe.recv_multipart()

            self.assertEqual(command, b'WHISPER')
            self.assertEqual(uuid, node1.uuid.bytes)
            self.assertEqual(name.decode(), node1.name)
            self.assertEqual(msg, [b'a', b'b'])

            node1.shout('test', [b'a', b'b'])
            command, uuid, name, group, *msg = node2.evt_pipe.recv_multipart()

            self.assertEqual(command, b'SHOUT')
            self.assertEqual(uuid, node1.uuid.bytes)
            self.assertEqual(name.decode(), node1.name)
            self.assertEqual(group.decode(), 'test')
            self.assertEqual(msg, [b'a', b'b'])

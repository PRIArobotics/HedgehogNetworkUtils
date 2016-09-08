import unittest
import zmq
from hedgehog.utils import discovery
from hedgehog.utils.discovery.node import Node, endpoint_to_port


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
        with Node("Node 1", ctx) as node1, \
                Node("Node 2", ctx) as node2:

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

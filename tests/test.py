import unittest
import zmq
from hedgehog.utils import zmq as zmq_utils
from hedgehog.utils import discovery


class ZmqTests(unittest.TestCase):
    def test_pipe(self):
        p1a, p1b = zmq_utils.pipe()

        msg = b'a'
        p1a.send(msg)
        p1b.send(p1b.recv())
        self.assertEqual(p1a.recv(), msg)

    def test_service_request_message(self):
        msg = discovery.service_request('test')
        self.assertEqual(msg.WhichOneof('command'), 'request')
        self.assertEqual(msg.request.service, 'test')

    def test_service_update_message(self):
        msg = discovery.service_update(ports=[1])
        self.assertEqual(msg.WhichOneof('command'), 'update')
        self.assertEqual(msg.update.service, '')
        self.assertEqual(msg.update.ports, [1])

        msg = discovery.service_update(ports={1})
        self.assertEqual(msg.WhichOneof('command'), 'update')
        self.assertEqual(msg.update.service, '')
        self.assertEqual(set(msg.update.ports), {1})

    def test_endpoint_to_port(self):
        port = discovery.endpoint_to_port(b'tcp://127.0.0.1:5555')
        self.assertEqual(port, 5555)

    def test_discovery(self):
        ctx = zmq.Context.instance()
        with discovery.Node("Node 1", ctx) as node1, \
                discovery.Node("Node 2", ctx) as node2:

            cmd, _, name, _, _ = node1.events.recv_multipart()
            self.assertEqual(cmd, b'ENTER')
            self.assertEqual(name, b'Node 2')

            cmd, _, name, _, _ = node2.events.recv_multipart()
            self.assertEqual(cmd, b'ENTER')
            self.assertEqual(name, b'Node 1')

            node2.join('hedgehog_server')
            node1.join('hedgehog_server')

            cmd, _, name, group = node1.events.recv_multipart()
            self.assertEqual(cmd, b'JOIN')
            self.assertEqual(name, b'Node 2')
            self.assertEqual(group, b'hedgehog_server')

            cmd, _, name, group = node2.events.recv_multipart()
            self.assertEqual(cmd, b'JOIN')
            self.assertEqual(name, b'Node 1')
            self.assertEqual(group, b'hedgehog_server')

            node2.add_service('hedgehog_server', 5555)

            cmd, _, name, group, message = node1.events.recv_multipart()
            self.assertEqual(cmd, b'SHOUT')
            self.assertEqual(name, b'Node 2')
            self.assertEqual(group, b'hedgehog_server')
            self.assertEqual(message, discovery.service_update(ports=[5555]).SerializeToString())

            node1.request_service('hedgehog_server')

            cmd, uuid, name, group, message = node2.events.recv_multipart()
            self.assertEqual(cmd, b'SHOUT')
            self.assertEqual(name, b'Node 1')
            self.assertEqual(group, b'hedgehog_server')
            self.assertEqual(message, discovery.service_request().SerializeToString())

            node2.peers[uuid].update_service('hedgehog_server')
            cmd, _, name, message = node1.events.recv_multipart()
            self.assertEqual(cmd, b'WHISPER')
            self.assertEqual(name, b'Node 2')
            self.assertEqual(message, discovery.service_update('hedgehog_server', [5555]).SerializeToString())

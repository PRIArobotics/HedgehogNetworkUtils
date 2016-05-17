import unittest
from hedgehog.utils import zmq as zmq_utils


class ZmqTests(unittest.TestCase):
    def test_pipe(self):
        p1a, p1b = zmq_utils.pipe()

        msg = b'a'
        p1a.send(msg)
        p1b.send(p1b.recv())
        self.assertEqual(p1a.recv(), msg)

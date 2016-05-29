import unittest
from . import protobuf_tests


class ProtobufTests(unittest.TestCase):
    def test_simple_message(self):
        old = protobuf_tests.Test(1)
        new = protobuf_tests.parse(old.serialize())
        self.assertEqual(new, old)

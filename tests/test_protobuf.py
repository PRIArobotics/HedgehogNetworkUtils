import unittest
from .protobuf_tests import Test, TestMessageType


class ProtobufTests(unittest.TestCase):
    def test_simple_message(self):
        old = Test(1)
        new = TestMessageType.parse(old.serialize())
        self.assertEqual(new, old)

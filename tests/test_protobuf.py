import unittest
from .protobuf_tests import Test, TestMessageType


class ProtobufTests(unittest.TestCase):
    def test_packed_message(self):
        old = Test(1)
        new = TestMessageType.parse(TestMessageType.serialize(old))
        self.assertEqual(new, old)

    def test_simple_message(self):
        old = Test(1)
        new = Test.parse(old.serialize())
        self.assertEqual(new, old)

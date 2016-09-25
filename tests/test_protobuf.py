import unittest
import protobuf_tests


class ProtobufTests(unittest.TestCase):
    def test_packed_message(self):
        old = protobuf_tests.Test(1)
        new = protobuf_tests.Msg.parse(protobuf_tests.Msg.serialize(old))
        self.assertEqual(new, old)

    def test_simple_message(self):
        old = protobuf_tests.Test(1)
        new = protobuf_tests.Test.parse(old.serialize())
        self.assertEqual(new, old)

import unittest
import protobuf_tests


class ProtobufTests(unittest.TestCase):
    def test_packed_message(self):
        old = protobuf_tests.Test(protobuf_tests.DEFAULT, 1)
        new = protobuf_tests.Msg1.parse(protobuf_tests.Msg1.serialize(old))
        self.assertEqual(new, old)
        new = protobuf_tests.Msg2.parse(protobuf_tests.Msg2.serialize(old))
        self.assertEqual(new, old)

    def test_simple_message(self):
        old = protobuf_tests.Test(protobuf_tests.DEFAULT, 1)
        new = protobuf_tests.Test.parse(old.serialize())
        self.assertEqual(new, old)

import unittest
import protobuf_tests
from protobuf_tests.proto import test_pb2


class ProtobufTests(unittest.TestCase):
    def test_serialize_packed_message(self):
        msg = protobuf_tests.Test(protobuf_tests.DEFAULT, 1)
        proto = protobuf_tests.Msg1.serialize(msg)

        expected = test_pb2.TestMessage1()
        expected.test.field = 1

        self.assertEqual(proto, expected.SerializeToString())

    def test_deserialize_packed_message(self):
        proto = test_pb2.TestMessage1()
        proto.test.field = 1
        msg = protobuf_tests.Msg1.parse(proto.SerializeToString())

        expected = protobuf_tests.Test(protobuf_tests.DEFAULT, 1)

        self.assertEqual(msg, expected)

    def test_serialize_simple_message(self):
        msg = protobuf_tests.Test(protobuf_tests.DEFAULT, 1)
        proto = msg.serialize()

        expected = test_pb2.Test()
        expected.field = 1

        self.assertEqual(proto, expected.SerializeToString())

    def test_deserialize_simple_message(self):
        proto = test_pb2.Test()
        proto.field = 1
        msg = protobuf_tests.Test.parse(proto.SerializeToString())

        expected = protobuf_tests.Test(protobuf_tests.DEFAULT, 1)

        self.assertEqual(msg, expected)

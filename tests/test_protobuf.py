from . import protobuf_tests
from .protobuf_tests.proto import test_pb2


class TestProtobuf(object):
    def test_serialize_packed_message(self):
        msg = protobuf_tests.DefaultTest(1)
        proto = protobuf_tests.Msg1.serialize(msg)

        expected = test_pb2.TestMessage1()
        expected.test.field = 1
        assert proto == expected.SerializeToString()

        msg = protobuf_tests.AlternativeTest(1)
        proto = protobuf_tests.Msg1.serialize(msg)

        expected = test_pb2.TestMessage1()
        expected.test.kind = protobuf_tests.ALTERNATIVE
        expected.test.field = 1
        assert proto == expected.SerializeToString()

        msg = protobuf_tests.SimpleTest(1)
        proto = protobuf_tests.Msg1.serialize(msg)

        expected = test_pb2.TestMessage1()
        expected.simple_test.field = 1
        assert proto == expected.SerializeToString()

    def test_deserialize_packed_message(self):
        proto = test_pb2.TestMessage1()
        proto.test.field = 1
        msg = protobuf_tests.Msg1.parse(proto.SerializeToString())

        expected = protobuf_tests.DefaultTest(1)
        assert msg == expected

        proto = test_pb2.TestMessage1()
        proto.test.kind = protobuf_tests.ALTERNATIVE
        proto.test.field = 1
        msg = protobuf_tests.Msg1.parse(proto.SerializeToString())

        expected = protobuf_tests.AlternativeTest(1)
        assert msg == expected

        proto = test_pb2.TestMessage1()
        proto.simple_test.field = 1
        msg = protobuf_tests.Msg1.parse(proto.SerializeToString())

        expected = protobuf_tests.SimpleTest(1)
        assert msg == expected

    def test_serialize_simple_message(self):
        msg = protobuf_tests.DefaultTest(1)
        proto = msg.serialize()

        expected = test_pb2.Test()
        expected.field = 1
        assert proto == expected.SerializeToString()

        msg = protobuf_tests.AlternativeTest(1)
        proto = msg.serialize()

        expected = test_pb2.Test()
        expected.kind = protobuf_tests.ALTERNATIVE
        expected.field = 1
        assert proto == expected.SerializeToString()

        msg = protobuf_tests.SimpleTest(1)
        proto = msg.serialize()

        expected = test_pb2.SimpleTest()
        expected.field = 1
        assert proto == expected.SerializeToString()

    def test_deserialize_simple_message(self):
        proto = test_pb2.Test()
        proto.field = 1
        msg = protobuf_tests.parse_test(proto.SerializeToString())

        expected = protobuf_tests.DefaultTest(1)
        assert msg == expected

        proto = test_pb2.Test()
        proto.kind = protobuf_tests.ALTERNATIVE
        proto.field = 1
        msg = protobuf_tests.parse_test(proto.SerializeToString())

        expected = protobuf_tests.AlternativeTest(1)
        assert msg == expected

        proto = test_pb2.SimpleTest()
        proto.field = 1
        msg = protobuf_tests.SimpleTest.parse(proto.SerializeToString())

        expected = protobuf_tests.SimpleTest(1)
        assert msg == expected

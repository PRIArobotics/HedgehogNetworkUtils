from hedgehog.utils.protobuf import ContainerMessage, Message, message
from .proto import test_pb2

from .proto.test_pb2 import DEFAULT, ALTERNATIVE

Msg1 = ContainerMessage(test_pb2.TestMessage1)
Msg2 = ContainerMessage(test_pb2.TestMessage2)


@message(test_pb2.Test, 'test')
class Test(Message):
    def __init__(self, kind, field):
        self.kind = kind
        self.field = field

    def _serialize(self, msg):
        msg.kind = self.kind
        msg.field = self.field


@Msg1.parser('test')
@Msg2.parser('test')
def _parse_test(msg):
    kind = msg.kind
    field = msg.field
    return Test(kind, field)


def parse_test(data):
    msg = test_pb2.Test()
    msg.ParseFromString(data)
    return _parse_test(msg)

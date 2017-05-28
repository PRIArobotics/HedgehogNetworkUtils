from hedgehog.utils.protobuf import ContainerMessage, Message, message
from .proto import test_pb2

from .proto.test_pb2 import DEFAULT, ALTERNATIVE

Msg1 = ContainerMessage(test_pb2.TestMessage1)
Msg2 = ContainerMessage(test_pb2.TestMessage2)


@message(test_pb2.Test, 'test', fields=('field',))
class DefaultTest(Message):
    def __init__(self, field):
        self.field = field

    def _serialize(self, msg):
        msg.kind = DEFAULT
        msg.field = self.field


@message(test_pb2.Test, 'test', fields=('field',))
class AlternativeTest(Message):
    def __init__(self, field):
        self.field = field

    def _serialize(self, msg):
        msg.kind = ALTERNATIVE
        msg.field = self.field


@Msg1.parser('test')
@Msg2.parser('test')
def _parse_test(msg):
    kind = msg.kind
    field = msg.field
    if kind == DEFAULT:
        return DefaultTest(field)
    else:
        return AlternativeTest(field)


def parse_test(data):
    msg = test_pb2.Test()
    msg.ParseFromString(data)
    return _parse_test(msg)

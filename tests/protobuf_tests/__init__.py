from hedgehog.utils.protobuf import ContainerMessage, Message
from .proto import test_pb2

from .proto.test_pb2 import DEFAULT, ALTERNATIVE

Msg1 = ContainerMessage(test_pb2.TestMessage1)
Msg2 = ContainerMessage(test_pb2.TestMessage2)


@Msg1.register(test_pb2.Test, 'test')
@Msg2.register(test_pb2.Test, 'test')
class Test(Message):
    def __init__(self, kind, field):
        self.kind = kind
        self.field = field

    @classmethod
    def _parse(cls, msg):
        kind = msg.kind
        field = msg.field
        return cls(kind, field)

    def _serialize(self, msg):
        msg.kind = self.kind
        msg.field = self.field

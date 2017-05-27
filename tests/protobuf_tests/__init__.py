from hedgehog.utils.protobuf import ContainerMessage, Message
from .proto import test_pb2


Msg1 = ContainerMessage(test_pb2.TestMessage1)
Msg2 = ContainerMessage(test_pb2.TestMessage2)


@Msg1.register(test_pb2.Test, 'test')
@Msg2.register(test_pb2.Test, 'test')
class Test(Message):
    def __init__(self, field):
        self.field = field

    @classmethod
    def _parse(cls, msg):
        field = msg.field
        return cls(field)

    def _serialize(self, msg):
        msg.field = self.field

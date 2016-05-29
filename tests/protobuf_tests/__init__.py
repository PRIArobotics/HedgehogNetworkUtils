from hedgehog.utils.protobuf import MessageType, Message
from .proto.test_pb2 import TestMessage, Test


TestMessageType = MessageType(TestMessage)
parse = TestMessageType.parse


@TestMessageType.register(Test, 'test')
class Test(Message):
    def __init__(self, field):
        self.field = field

    @classmethod
    def _parse(cls, msg):
        return cls(msg.field)

    def _serialize(self, msg):
        msg.field = self.field

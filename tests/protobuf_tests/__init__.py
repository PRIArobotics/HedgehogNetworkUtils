from hedgehog.utils.protobuf import MessageType, Message
from .proto.test_pb2 import TestMessage


TestMessageType = MessageType(TestMessage)


parse = TestMessageType.parse


@TestMessageType.register
class Test(Message):
    discriminator = 'test'
    name = 'Test'
    fields = ('field',)
    type = TestMessage

    def __init__(self, field):
        self.field = field

    @classmethod
    def _parse(cls, msg):
        return cls(msg.field)

    def _serialize(self, msg):
        msg.field = self.field

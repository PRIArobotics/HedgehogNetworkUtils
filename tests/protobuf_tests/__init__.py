from .proto.test_pb2 import TestMessage


registry = {}


def register(class_):
    registry[class_.payload_oneof] = class_
    return class_


def parse(data):
    msg = TestMessage()
    msg.ParseFromString(data)
    key = msg.WhichOneof('payload')
    msg_type = registry[key]
    return msg_type.parse(msg)


class Message:
    payload_oneof = None
    name = None
    fields = None
    async = False

    @classmethod
    def _get_oneof(cls, msg):
        return getattr(msg, cls.payload_oneof)

    @classmethod
    def _parse(cls, msg):
        raise NotImplementedError

    def _serialize(self, msg):
        raise NotImplementedError

    @classmethod
    def parse(cls, msg):
        return cls._parse(cls._get_oneof(msg))

    def serialize(self):
        msg = TestMessage()
        self._serialize(self._get_oneof(msg))
        return msg.SerializeToString()

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        for field in self.fields:
            if getattr(self, field) != getattr(other, field):
                return False
        return True

    def __repr__(self):
        field_pairs = ((field, getattr(self, field)) for field in self.fields)
        field_reprs = ('{}={}'.format(field, repr(value)) for field, value in field_pairs if value)
        return '{}({})'.format(self.name, ', '.join(field_reprs))


@register
class Test(Message):
    payload_oneof = 'test'
    name = 'Test'
    fields = ('field',)

    def __init__(self, field):
        self.field = field

    @classmethod
    def _parse(cls, msg):
        return cls(msg.field)

    def _serialize(self, msg):
        msg.field = self.field

from collections import namedtuple


MessageMeta = namedtuple('MessageMeta', ('discriminator', 'type', 'name', 'fields'))


class MessageType:
    def __init__(self, type):
        self.registry = {}
        self.type = type

    def register(self, proto_message_class, discriminator):
        def decorator(message_class):
            desc = proto_message_class.DESCRIPTOR

            message_class.meta = MessageMeta(discriminator, proto_message_class, desc.name,
                                             tuple(field.name for field in desc.fields))

            self.registry[message_class.meta.discriminator] = message_class
            return message_class
        return decorator

    def parse(self, data):
        msg = self.type()
        msg.ParseFromString(data)
        discriminator = msg.WhichOneof('payload')
        msg_type = self.registry[discriminator]
        return msg_type._parse(getattr(msg, discriminator))

    def serialize(self, instance):
        msg = self.type()
        instance.serialize(getattr(msg, instance.meta.discriminator))
        return msg.SerializeToString()


class Message:
    meta = None

    @classmethod
    def _parse(cls, msg):
        raise NotImplementedError

    def _serialize(self, msg):
        raise NotImplementedError

    @classmethod
    def parse(cls, data):
        msg = cls.meta.type()
        msg.ParseFromString(data)
        return cls._parse(msg)

    def serialize(self, msg=None):
        msg = msg or self.meta.type()
        self._serialize(msg)
        return msg.SerializeToString()

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        for field in self.meta.fields:
            if getattr(self, field) != getattr(other, field):
                return False
        return True

    def __repr__(self):
        field_pairs = ((field, getattr(self, field)) for field in self.meta.fields)
        field_reprs = ('{}={}'.format(field, repr(value)) for field, value in field_pairs if value)
        return '{}({})'.format(self.meta.name, ', '.join(field_reprs))

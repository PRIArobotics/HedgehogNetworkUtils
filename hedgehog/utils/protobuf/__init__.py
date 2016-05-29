class MessageType:
    def __init__(self, type):
        self.registry = {}
        self.type = type

    def register(self, proto_message_class, discriminator):
        def decorator(message_class):
            desc = proto_message_class.DESCRIPTOR

            message_class.discriminator = discriminator
            message_class.type = proto_message_class
            message_class.name = desc.name
            message_class.fields = tuple(field.name for field in desc.fields)

            self.registry[message_class.discriminator] = message_class
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
        instance.serialize(getattr(msg, instance.discriminator))
        return msg.SerializeToString()


class Message:
    discriminator = None
    type = None
    name = None
    fields = None

    @classmethod
    def _parse(cls, msg):
        raise NotImplementedError

    def _serialize(self, msg):
        raise NotImplementedError

    @classmethod
    def parse(cls, data):
        msg = cls.type()
        msg.ParseFromString(data)
        return cls._parse(msg)

    def serialize(self, msg=None):
        msg = msg or self.type()
        self._serialize(msg)
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

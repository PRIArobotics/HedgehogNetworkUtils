from collections import namedtuple


MessageMeta = namedtuple('MessageMeta', ('discriminator', 'proto_class', 'fields'))


def message(proto_class, discriminator, fields=None):
    if fields is None:
        fields = tuple(field.name for field in proto_class.DESCRIPTOR.fields)

    meta = MessageMeta(discriminator, proto_class, fields)

    def decorator(message_class):
        message_class.meta = meta
        return message_class

    return decorator


class ContainerMessage(object):
    def __init__(self, proto_class):
        self.registry = {}
        self.proto_class = proto_class

    def message(self, proto_class, discriminator, fields=None):
        message_decorator = message(proto_class, discriminator, fields)
        parser_decorator = self.parser(discriminator)

        def decorator(message_class):
            message_class = message_decorator(message_class)
            parser_decorator(message_class._parse)
            return message_class
        return decorator

    def parser(self, discriminator):
        def decorator(parse_fn):
            self.registry[discriminator] = parse_fn
            return parse_fn
        return decorator

    def parse(self, data):
        msg = self.proto_class()
        msg.ParseFromString(data)
        discriminator = msg.WhichOneof('payload')
        parse_fn = self.registry[discriminator]
        return parse_fn(getattr(msg, discriminator))

    def serialize(self, instance):
        msg = self.proto_class()
        instance.serialize(getattr(msg, instance.meta.discriminator))
        return msg.SerializeToString()


class Message(object):
    meta = None

    def _serialize(self, msg):
        raise NotImplementedError

    def serialize(self, msg=None):
        msg = msg or self.meta.proto_class()
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
        field_reprs = ('{}={}'.format(field, repr(value)) for field, value in field_pairs)
        return '{}({})'.format(self.__class__.__name__, ', '.join(field_reprs))


class SimpleMessageMixin(object):
    @classmethod
    def _parse(cls, msg):
        raise NotImplementedError

    @classmethod
    def parse(cls, data):
        msg = cls.meta.proto_class()
        msg.ParseFromString(data)
        return cls._parse(msg)

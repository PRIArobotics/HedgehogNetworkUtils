from typing import cast, Callable, Dict, Iterable, Type, TypeVar

from collections import namedtuple

from google.protobuf.message import Message as ProtoMessage


MessageMeta = namedtuple('MessageMeta', ('discriminator', 'proto_class', 'fields'))


def message(proto_class: Type[ProtoMessage], discriminator: str, fields: Iterable[str]=None)\
        -> Callable[[Type['Message']], Type['Message']]:
    if fields is None:
        fields = tuple(field.name for field in proto_class.DESCRIPTOR.fields)

    meta = MessageMeta(discriminator, proto_class, fields)

    def decorator(message_class: Type[Message]) -> Type[Message]:
        message_class.meta = meta
        return message_class

    return decorator


class ContainerMessage(object):
    def __init__(self, proto_class: Type[ProtoMessage]) -> None:
        self.registry = {}  # type: Dict[str, Callable[[ProtoMessage], Message]]
        self.proto_class = proto_class

    def message(self, proto_class: Type[ProtoMessage], discriminator: str, fields: Iterable[str]=None)\
            -> Callable[[Type['Message']], Type['Message']]:
        message_decorator = message(proto_class, discriminator, fields)
        parser_decorator = self.parser(discriminator)

        def decorator(message_class: Type[Message]) -> Type[Message]:
            message_class = message_decorator(message_class)
            parser_decorator(cast(SimpleMessageMixin, message_class)._parse)
            return message_class
        return decorator

    def parser(self, discriminator: str)\
            -> Callable[[Callable[[ProtoMessage], 'Message']], Callable[[ProtoMessage], 'Message']]:
        def decorator(parse_fn: Callable[[ProtoMessage], Message]) -> Callable[[ProtoMessage], Message]:
            self.registry[discriminator] = parse_fn
            return parse_fn
        return decorator

    def parse(self, data: bytes) -> 'Message':
        msg = self.proto_class()
        msg.ParseFromString(data)
        discriminator = msg.WhichOneof('payload')
        parse_fn = self.registry[discriminator]
        return parse_fn(getattr(msg, discriminator))

    def serialize(self, instance: 'Message') -> bytes:
        msg = self.proto_class()
        instance.serialize(getattr(msg, instance.meta.discriminator))
        return msg.SerializeToString()


class Message(object):
    meta = None  # type: MessageMeta

    def _serialize(self, msg: ProtoMessage) -> None:
        raise NotImplementedError()  # pragma: no cover

    def serialize(self, msg: ProtoMessage=None) -> bytes:
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
    def _parse(cls, msg: ProtoMessage) -> Message:
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    def parse(cls, data: bytes) -> Message:
        msg = cast(Message, cls).meta.proto_class()
        msg.ParseFromString(data)
        return cls._parse(msg)

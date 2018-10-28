from typing import cast, Callable, Dict, Iterable, Type, TypeVar

from dataclasses import dataclass

from google.protobuf.message import Message as ProtoMessage


ParseFn = Callable[[ProtoMessage], 'Message']
T = TypeVar('T')
SimpleDecorator = Callable[[T], T]


@dataclass(frozen=True)
class message:
    proto_class: Type[ProtoMessage]
    discriminator: str
    fields: Iterable[str] = None

    def __post_init__(self):
        if self.fields is None:
            fields = tuple(field.name for field in self.proto_class.DESCRIPTOR.fields)
            object.__setattr__(self, 'fields', fields)

    def __call__(self, message_class: Type['Message']) -> Type['Message']:
        message_class.meta = self
        return message_class


class ContainerMessage(object):
    def __init__(self, proto_class: Type[ProtoMessage]) -> None:
        self.parse_fns = {}  # type: Dict[str, ParseFn]
        self.proto_class = proto_class

    def message(self, proto_class: Type[ProtoMessage], discriminator: str, fields: Iterable[str]=None)\
            -> SimpleDecorator[Type['Message']]:
        message_decorator = message(proto_class, discriminator, fields)
        parser_decorator = self.parser(discriminator)

        def decorator(message_class: Type[Message]) -> Type[Message]:
            message_class = message_decorator(message_class)
            parser_decorator(cast(SimpleMessageMixin, message_class)._parse)
            return message_class
        return decorator

    def parser(self, discriminator: str) -> SimpleDecorator[ParseFn]:
        def decorator(parse_fn: ParseFn) -> ParseFn:
            self.parse_fns[discriminator] = parse_fn
            return parse_fn
        return decorator

    def parse(self, data: bytes) -> 'Message':
        msg = self.proto_class()
        msg.ParseFromString(data)
        discriminator = cast(str, msg.WhichOneof('payload'))
        parse_fn = self.parse_fns[discriminator]
        return parse_fn(getattr(msg, discriminator))

    def serialize(self, instance: 'Message') -> bytes:
        msg = self.proto_class()
        instance.serialize(getattr(msg, instance.meta.discriminator))
        return msg.SerializeToString()


class Message(object):
    meta = None  # type: message

    def _serialize(self, msg: ProtoMessage) -> None:
        raise NotImplementedError()  # pragma: no cover

    def serialize(self, msg: ProtoMessage=None) -> bytes:
        msg = msg or self.meta.proto_class()
        self._serialize(msg)
        return msg.SerializeToString()


class SimpleMessageMixin(object):
    @classmethod
    def _parse(cls, msg: ProtoMessage) -> Message:
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    def parse(cls, data: bytes) -> Message:
        msg = cast(Message, cls).meta.proto_class()
        msg.ParseFromString(data)
        return cls._parse(msg)

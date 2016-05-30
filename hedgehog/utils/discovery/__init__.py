from hedgehog.utils.protobuf import MessageType, Message
from .proto import discovery_pb2, node_api_pb2


Msg = MessageType(discovery_pb2.HedgehogDiscoveryMessage)
ApiMsg = MessageType(node_api_pb2.NodeApiMessage)


@Msg.register(discovery_pb2.ServiceRequest, 'request')
class Request(Message):
    def __init__(self, service=''):
        self.service = service

    @classmethod
    def _parse(cls, msg):
        return cls(msg.service)

    def _serialize(self, msg):
        msg.service = self.service


@Msg.register(discovery_pb2.ServiceUpdate, 'update')
class Update(Message):
    def __init__(self, service='', ports=()):
        self.service = service
        self.ports = set(ports)

    @classmethod
    def _parse(cls, msg):
        return cls(msg.service, msg.ports)

    def _serialize(self, msg):
        msg.service = self.service
        msg.ports.extend(self.ports)


@ApiMsg.register(node_api_pb2.EnterMessage, 'enter')
class Enter(Message):
    def __init__(self, uuid, name, headers, address):
        self.uuid = uuid
        self.name = name
        self.headers = headers
        self.address = address

    @classmethod
    def _parse(cls, msg):
        return cls(msg.uuid, msg.name, msg.headers, msg.address)

    def _serialize(self, msg):
        msg.uuid = self.uuid
        msg.name = self.name
        msg.headers = self.headers
        msg.address = self.address


@ApiMsg.register(node_api_pb2.ExitMessage, 'exit')
class Exit(Message):
    def __init__(self, uuid, name):
        self.uuid = uuid
        self.name = name

    @classmethod
    def _parse(cls, msg):
        return cls(msg.uuid, msg.name)

    def _serialize(self, msg):
        msg.uuid = self.uuid
        msg.name = self.name


@ApiMsg.register(node_api_pb2.JoinMessage, 'join')
class Join(Message):
    def __init__(self, uuid, name, group):
        self.uuid = uuid
        self.name = name
        self.group = group

    @classmethod
    def _parse(cls, msg):
        return cls(msg.uuid, msg.name, msg.group)

    def _serialize(self, msg):
        msg.uuid = self.uuid
        msg.name = self.name
        msg.group = self.group


@ApiMsg.register(node_api_pb2.LeaveMessage, 'leaver')
class Leave(Message):
    def __init__(self, uuid, name, group):
        self.uuid = uuid
        self.name = name
        self.group = group

    @classmethod
    def _parse(cls, msg):
        return cls(msg.uuid, msg.name, msg.group)

    def _serialize(self, msg):
        msg.uuid = self.uuid
        msg.name = self.name
        msg.group = self.group


@ApiMsg.register(node_api_pb2.ShoutMessage, 'shout')
class Shout(Message):
    def __init__(self, uuid, name, group, payload):
        self.uuid = uuid
        self.name = name
        self.group = group
        self.payload = payload

    @classmethod
    def _parse(cls, msg):
        return cls(msg.uuid, msg.name, msg.group, msg.payload)

    def _serialize(self, msg):
        msg.uuid = self.uuid
        msg.name = self.name
        msg.group = self.group
        msg.payload = self.payload


@ApiMsg.register(node_api_pb2.WhisperMessage, 'whisper')
class Whisper(Message):
    def __init__(self, uuid, name, payload):
        self.uuid = uuid
        self.name = name
        self.payload = payload

    @classmethod
    def _parse(cls, msg):
        return cls(msg.uuid, msg.name, msg.payload)

    def _serialize(self, msg):
        msg.uuid = self.uuid
        msg.name = self.name
        msg.payload = self.payload


@ApiMsg.register(node_api_pb2.ServiceMessage, 'service')
class Service(Message):
    def __init__(self, service, endpoints=()):
        self.service = service
        self.endpoints = set(endpoints)

    @classmethod
    def _parse(cls, msg):
        return cls(msg.service, msg.endpoints)

    def _serialize(self, msg):
        msg.service = self.service
        msg.endpoints.extend(self.endpoints)


@ApiMsg.register(node_api_pb2.RegisterServiceMessage, 'register_service')
class RegisterService(Message):
    def __init__(self, service, added_ports=(), removed_ports=()):
        self.service = service
        self.added_ports = set(added_ports)
        self.removed_ports = set(removed_ports)

    @classmethod
    def _parse(cls, msg):
        return cls(msg.service, msg.added_ports, msg.removed_ports)

    def _serialize(self, msg):
        msg.service = self.service
        msg.added_ports.extend(self.added_ports)
        msg.removed_ports.extend(self.removed_ports)

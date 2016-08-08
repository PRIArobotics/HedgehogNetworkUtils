from hedgehog.utils.protobuf import MessageType, Message
from .proto import discovery_pb2


Msg = MessageType(discovery_pb2.HedgehogDiscoveryMessage)


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


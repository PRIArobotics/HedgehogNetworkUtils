from hedgehog.utils.protobuf import ContainerMessage, Message, message
from .proto import discovery_pb2


Msg = ContainerMessage(discovery_pb2.HedgehogDiscoveryMessage)


@message(discovery_pb2.ServiceRequest, 'request')
class Request(Message):
    def __init__(self, service=''):
        self.service = service

    def _serialize(self, msg):
        msg.service = self.service


@Msg.parser('request')
def _parse_request(msg):
    return Request(msg.service)


def parse_request(data):
    msg = discovery_pb2.ServiceRequest()
    msg.ParseFromString(data)
    return _parse_request(msg)


@message(discovery_pb2.ServiceUpdate, 'update')
class Update(Message):
    def __init__(self, service='', ports=()):
        self.service = service
        self.ports = set(ports)

    def _serialize(self, msg):
        msg.service = self.service
        msg.ports.extend(self.ports)


@Msg.parser('update')
def _parse_update(msg):
    return Update(msg.service, msg.ports)


def parse_update(data):
    msg = discovery_pb2.ServiceUpdate()
    msg.ParseFromString(data)
    return _parse_update(msg)

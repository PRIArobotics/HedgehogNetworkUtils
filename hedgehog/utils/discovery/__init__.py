from collections import defaultdict
import zmq
import uuid

from .proto.discovery_pb2 import HedgehogDiscoveryMessage, ServiceRequest, ServiceUpdate
from pyre.pyre import Pyre


def service_request(service=''):
    msg = HedgehogDiscoveryMessage()
    msg.request.service = service
    return msg


def service_update(service='', ports=None):
    msg = HedgehogDiscoveryMessage()
    msg.update.service = service
    if ports is not None:
        msg.update.ports.extend(ports)
    return msg


def endpoint_to_port(endpoint):
    if isinstance(endpoint, zmq.Socket):
        endpoint = endpoint.last_endpoint
    if isinstance(endpoint, bytes):
        endpoint = int(endpoint.rsplit(b':', 1)[-1])

    if isinstance(endpoint, int):
        return endpoint
    else:
        raise ValueError(endpoint)


class Node(Pyre):
    def __init__(self, name=None, ctx=None, *args, **kwargs):
        super().__init__(name, ctx, *args, **kwargs)
        self.services = defaultdict(set)

    def add_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        ports = self.services[service]
        ports.add(port)
        self.shout(service, service_update(ports=ports).SerializeToString())

    def remove_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        ports = self.services[service]
        ports.remove(port)
        self.shout(service, service_update(ports=ports).SerializeToString())

    def request_service(self, service):
        self.shout(service, service_request().SerializeToString())

    def update_service(self, service, peer):
        ports = self.services[service]
        self.whisper(uuid.UUID(bytes=peer), service_update(service, ports).SerializeToString())

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

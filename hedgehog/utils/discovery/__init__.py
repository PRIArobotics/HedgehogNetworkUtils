from collections import defaultdict
import zmq
import threading
from uuid import UUID

from hedgehog.utils import zmq as zmq_utils
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
    class Peer:
        def __init__(self, node, name, uuid, address):
            self.node = node
            self.name = name
            self.uuid = UUID(bytes=uuid)
            self.address = address
            self.services = {}

        def request_service(self, service):
            self.node.whisper(self.uuid, service_request(service).SerializeToString())

        def update_service(self, service):
            ports = self.node.services[service]
            self.node.whisper(self.uuid, service_update(service, ports).SerializeToString())

    def __init__(self, name=None, ctx=None, *args, **kwargs):
        super().__init__(name, ctx, *args, **kwargs)
        self.services = defaultdict(set)
        self.peers = {}

        self.events, events = zmq_utils.pipe(ctx)

        def inbox_handler():
            parts = self.inbox.recv_multipart()
            cmd, uuid, name, *payload = parts
            name = name.decode('utf-8')

            def decode(frame):
                msg = HedgehogDiscoveryMessage()
                msg.ParseFromString(frame)
                return msg

            def handle_request(peer, service):
                peer.update_service(service)

            def handle_update(peer, service, ports):
                peer.services[service] = set(ports)

            if cmd == b'ENTER':
                _, address = payload
                address = address.decode('utf-8')

                self.add_peer(name, uuid, address)
            elif cmd == b'EXIT':
                del self.peers[uuid]
            elif cmd == b'JOIN':
                group, = payload
                group = group.decode('utf-8')
                peer = self.peers[uuid]

                peer.services[group] = set()
            elif cmd == b'LEAVE':
                group, = payload
                group = group.decode('utf-8')
                peer = self.peers[uuid]

                del peer.services[group]
            elif cmd == b'SHOUT':
                group, message = payload
                group = group.decode('utf-8')
                message = decode(message)
                peer = self.peers[uuid]

                if message.WhichOneof('command') == 'request':
                    handle_request(peer, message.request.service or group)
                elif message.WhichOneof('command') == 'update':
                    handle_update(peer, message.update.service or group, message.update.ports)
            elif cmd == b'WHISPER':
                message, = payload
                message = decode(message)
                peer = self.peers[uuid]

                if message.WhichOneof('command') == 'request':
                    handle_request(peer, message.request.service)
                elif message.WhichOneof('command') == 'update':
                    handle_update(peer, message.update.service, message.update.ports)

            events.send_multipart(parts)

        def kill_handler():
            events.recv()
            for socket in list(poller.sockets):
                socket.close()
                poller.unregister(socket)

        poller = zmq_utils.Poller()
        poller.register(self.inbox, zmq.POLLIN, inbox_handler)
        poller.register(events, zmq.POLLIN, kill_handler)

        def poll():
            while len(poller.sockets) > 0:
                for _, _, handler in poller.poll():
                    handler()

        threading.Thread(target=poll).start()

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

    def add_peer(self, name, uuid, address):
        peer = Node.Peer(self, name, uuid, address)
        self.peers[uuid] = peer
        return peer

    def stop(self):
        super().stop()
        self.events.send(b'')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

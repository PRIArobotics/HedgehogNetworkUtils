from collections import defaultdict
import zmq
import threading
from uuid import UUID

from hedgehog.utils import zmq as zmq_utils
from hedgehog.utils.protobuf import MessageType, Message
from .proto import discovery_pb2
from pyre.pyre import Pyre


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
            self.host_address = address.rsplit(':', 1)[0]
            self.services = {}

        def request_service(self, service):
            self.node.whisper(self.uuid, Msg.serialize(Request(service)))

        def update_service(self, service):
            ports = self.node.services[service]
            self.node.whisper(self.uuid, Msg.serialize(Update(service, ports)))

    def __init__(self, name=None, ctx=None, *args, **kwargs):
        super().__init__(name, ctx, *args, **kwargs)
        self.services = defaultdict(set)
        self.peers = {}

        self.events, events = zmq_utils.pipe(ctx)

        def inbox_handler():
            msgs = []

            parts = self.inbox.recv_multipart()
            msgs.append(parts)

            cmd, uuid, name, *payload = parts
            name = name.decode('utf-8')

            def handle_request(peer, service):
                peer.update_service(service)

            def handle_update(peer, service, ports):
                peer.services[service] = {"{}:{}".format(peer.host_address, port) for port in ports}

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
                message = Msg.parse(message)
                peer = self.peers[uuid]

                service = message.service or group
                if isinstance(message, Request):
                    handle_request(peer, service)
                elif isinstance(message, Update):
                    handle_request(peer, message.request.service or group)
            elif cmd == b'WHISPER':
                message, = payload
                message = Msg.parse(message)
                peer = self.peers[uuid]

                service = message.service
                if isinstance(message, Request):
                    handle_request(peer, service)
                elif isinstance(message, Update):
                    handle_update(peer, message.update.service, message.update.ports)

            for msg in msgs:
                events.send_multipart(msg)

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
        self.shout(service, Msg.serialize(Update(ports=ports)))

    def remove_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        ports = self.services[service]
        ports.remove(port)
        self.shout(service, Msg.serialize(Update(ports=ports)))

    def request_service(self, service):
        self.shout(service, Msg.serialize(Request()))

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

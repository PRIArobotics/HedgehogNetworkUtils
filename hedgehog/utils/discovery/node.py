from collections import defaultdict
from uuid import UUID
import threading

import zmq
from pyre.pyre import Pyre

from hedgehog.utils import zmq as zmq_utils
from .. import discovery

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
            self.node.whisper(self.uuid, discovery.Msg.serialize(discovery.Request(service)))

        def update_service(self, service):
            ports = self.node.services[service]
            self.node.whisper(self.uuid, discovery.Msg.serialize(discovery.Update(service, ports)))

    def __init__(self, name=None, ctx=None, *args, **kwargs):
        super().__init__(name, ctx, *args, **kwargs)
        self.services = defaultdict(set)
        self.peers = {}

        self.events, events = zmq_utils.pipe(ctx)

        def inbox_handler():
            msgs = []

            cmd, uuid, name, *payload = self.inbox.recv_multipart()
            name = name.decode('utf-8')

            def handle_request(peer, service):
                peer.update_service(service)

            def handle_update(peer, service, ports):
                peer.services[service] = {"{}:{}".format(peer.host_address, port) for port in ports}

                endpoints = [endpoint
                             for peer in self.peers.values()
                             for endpoint in peer.services[service]]
                return discovery.Service(service.encode('utf-8'), endpoints)

            if cmd == b'ENTER':
                headers, address = payload
                address = address.decode('utf-8')

                msgs.append(discovery.Enter(uuid, name, headers, address))

                self.add_peer(name, uuid, address)
            elif cmd == b'EXIT':
                del self.peers[uuid]

                msgs.append(discovery.Exit(uuid, name))
            elif cmd == b'JOIN':
                group, = payload
                group = group.decode('utf-8')

                msgs.append(discovery.Join(uuid, name, group))

                peer = self.peers[uuid]
                peer.services[group] = set()
            elif cmd == b'LEAVE':
                group, = payload
                group = group.decode('utf-8')

                msgs.append(discovery.Leave(uuid, name, group))

                peer = self.peers[uuid]
                del peer.services[group]
            elif cmd == b'SHOUT':
                group, message = payload
                group = group.decode('utf-8')

                msgs.append(discovery.Shout(uuid, name, group, message))

                peer = self.peers[uuid]
                message = discovery.Msg.parse(message)
                service = message.service or group

                if isinstance(message, discovery.Request):
                    handle_request(peer, service)
                elif isinstance(message, discovery.Update):
                    msgs.append(handle_update(peer, service, message.ports))
            elif cmd == b'WHISPER':
                message, = payload

                msgs.append(discovery.Whisper(uuid, name, message))

                peer = self.peers[uuid]
                message = discovery.Msg.parse(message)
                service = message.service

                if isinstance(message, discovery.Request):
                    handle_request(peer, service)
                elif isinstance(message, discovery.Update):
                    msgs.append(handle_update(peer, service, message.ports))

            for msg in msgs:
                events.send(discovery.ApiMsg.serialize(msg))

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
        self.shout(service, discovery.Msg.serialize(discovery.Update(ports=ports)))

    def remove_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        ports = self.services[service]
        ports.remove(port)
        self.shout(service, discovery.Msg.serialize(discovery.Update(ports=ports)))

    def request_service(self, service):
        self.shout(service, discovery.Msg.serialize(discovery.Request()))

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

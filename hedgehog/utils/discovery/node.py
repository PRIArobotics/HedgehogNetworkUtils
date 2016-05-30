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

        def inbox_handle_enter(msgs, uuid, name, headers, address):
            name = name.decode('utf-8')
            address = address.decode('utf-8')

            msgs.append(discovery.Enter(uuid, name, headers, address))

            self.add_peer(name, uuid, address)

        def inbox_handle_exit(msgs, uuid, name):
            name = name.decode('utf-8')

            del self.peers[uuid]

            msgs.append(discovery.Exit(uuid, name))

        def inbox_handle_join(msgs, uuid, name, group):
            name = name.decode('utf-8')
            group = group.decode('utf-8')

            msgs.append(discovery.Join(uuid, name, group))

            peer = self.peers[uuid]
            peer.services[group] = set()

        def inbox_handle_leave(msgs, uuid, name, group):
            name = name.decode('utf-8')
            group = group.decode('utf-8')

            msgs.append(discovery.Leave(uuid, name, group))

            peer = self.peers[uuid]
            del peer.services[group]

        def handle_update(msgs, peer, service, ports):
            peer.services[service] = {"{}:{}".format(peer.host_address, port) for port in ports}

            endpoints = [endpoint
                         for peer in self.peers.values()
                         for endpoint in peer.services[service]]
            msgs.append(discovery.Service(service.encode('utf-8'), endpoints))

        def inbox_handle_shout(msgs, uuid, name, group, message):
            name = name.decode('utf-8')
            group = group.decode('utf-8')

            msgs.append(discovery.Shout(uuid, name, group, message))

            peer = self.peers[uuid]
            message = discovery.Msg.parse(message)
            service = message.service or group

            if isinstance(message, discovery.Request):
                peer.update_service(service)
            elif isinstance(message, discovery.Update):
                handle_update(msgs, peer, service, message.ports)

        def inbox_handle_whisper(msgs, uuid, name, message):
            name = name.decode('utf-8')

            msgs.append(discovery.Whisper(uuid, name, message))

            peer = self.peers[uuid]
            message = discovery.Msg.parse(message)
            service = message.service

            if isinstance(message, discovery.Request):
                peer.update_service(service)
            elif isinstance(message, discovery.Update):
                handle_update(msgs, peer, service, message.ports)

        inbox_handlers = {
            b'ENTER': inbox_handle_enter,
            b'EXIT': inbox_handle_exit,
            b'JOIN': inbox_handle_join,
            b'LEAVE': inbox_handle_leave,
            b'SHOUT': inbox_handle_shout,
            b'WHISPER': inbox_handle_whisper,
        }

        def inbox_handler():
            msgs = []

            cmd, *payload = self.inbox.recv_multipart()
            inbox_handlers[cmd](msgs, *payload)

            for msg in msgs:
                events.send(discovery.ApiMsg.serialize(msg))

        def events_handle_exit():
            for socket in list(poller.sockets):
                socket.close()
                poller.unregister(socket)

        def events_handle_api(msg):
            msg = discovery.ApiMsg.parse(msg)

            if isinstance(msg, discovery.RegisterService):
                ports = self.services[msg.service]
                ports |= msg.added_ports
                ports -= msg.removed_ports
                self.shout(msg.service, discovery.Msg.serialize(discovery.Update(ports=ports)))

        def events_handle_out(group, msg):
            self.shout(group.decode('utf-8'), msg)

        events_handlers = {
            b'EXIT': events_handle_exit,
            b'API': events_handle_api,
            b'OUT': events_handle_out,
        }

        def events_handler():
            cmd, *payload = events.recv_multipart()
            events_handlers[cmd](*payload)

        poller = zmq_utils.Poller()
        poller.register(self.inbox, zmq.POLLIN, inbox_handler)
        poller.register(events, zmq.POLLIN, events_handler)

        def poll():
            while len(poller.sockets) > 0:
                for _, _, handler in poller.poll():
                    handler()

        threading.Thread(target=poll).start()

    def add_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        self.events.send_multipart(
            [b'API', discovery.ApiMsg.serialize(discovery.RegisterService(service, added_ports={port}))])

    def remove_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        self.events.send_multipart(
            [b'API', discovery.ApiMsg.serialize(discovery.RegisterService(service, removed_ports={port}))])

    def request_service(self, service):
        self.events.send_multipart(
            [b'OUT', service.encode('utf-8'), discovery.Msg.serialize(discovery.Request())])

    def add_peer(self, name, uuid, address):
        peer = Node.Peer(self, name, uuid, address)
        self.peers[uuid] = peer
        return peer

    def stop(self):
        super().stop()
        self.events.send(b'EXIT')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

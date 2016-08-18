from collections import defaultdict

import zmq
import pickle
import logging
from pyre import zhelper
from pyre.pyre import Pyre
from pyre.zactor import ZActor

from hedgehog.utils import zmq as zmq_utils
from .. import discovery

logger = logging.getLogger(__name__)


def endpoint_to_port(endpoint):
    if isinstance(endpoint, zmq.Socket):
        endpoint = endpoint.last_endpoint
    if isinstance(endpoint, bytes):
        endpoint = int(endpoint.rsplit(b':', 1)[-1])

    if isinstance(endpoint, int):
        return endpoint
    else:
        raise ValueError(endpoint)


class NodeActor(object):
    class Peer:
        def __init__(self, node, name, uuid, address):
            self.node = node
            self.name = name
            self.uuid = uuid
            self.address = address
            self.host_address = address.rsplit(':', 1)[0]
            self.services = defaultdict(set)

        def copy(self, service=None):
            result = NodeActor.Peer(None, self.name, self.uuid, self.address)
            if service is None:
                result.services = dict(self.services)
            else:
                result.services = {service: self.services[service]}
            return result

        def request_service(self, service):
            self.node.actor.send_unicode("WHISPER", flags=zmq.SNDMORE)
            self.node.actor.send(self.uuid, flags=zmq.SNDMORE)
            self.node.actor.send(discovery.Msg.serialize(discovery.Request(service)))

        def update_service(self, service):
            ports = self.node.services[service]
            self.node.actor.send_unicode("WHISPER", flags=zmq.SNDMORE)
            self.node.actor.send(self.uuid, flags=zmq.SNDMORE)
            self.node.actor.send(discovery.Msg.serialize(discovery.Update(service, ports)))

    def __init__(self, ctx, pipe, queue, actor, outbox, backend):
        self.pipe = pipe
        self.queue = queue
        self.actor = actor
        self.outbox = outbox
        self.backend = backend

        self.services = defaultdict(set)
        self.peers = {}

        backend_handlers = {
            "ENTER": self.recv_backend_enter,
            "EXIT": self.recv_backend_exit,
            "JOIN": self.recv_backend_join,
            "LEAVE": self.recv_backend_leave,
            "SHOUT": self.recv_backend_shout,
            "WHISPER": self.recv_backend_whisper,
            "STOP": lambda uuid, name: None,
            "$TERM": self.recv_backend_term,
        }

        def recv_backend():
            event = self.backend.recv_multipart()
            command = event[0].decode('UTF-8')
            backend_handlers[command](*event[1:])
            if command not in {"STOP", "$TERM"}:
                self.outbox.send_multipart(event)

        self.poller = zmq_utils.Poller()
        # pipe for API commands from client
        self.poller.register(self.pipe, zmq.POLLIN, self.recv_api)
        # pipe for API commands to PyreNode
        self.poller.register(self.actor.resolve(), zmq.POLLIN, lambda: self.pipe.send_multipart(self.actor.recv_multipart()))
        # pipe for events from Pyre
        self.poller.register(self.backend, zmq.POLLIN, recv_backend)

        self.pipe.signal()

        while len(self.poller.sockets) > 0:
            for _, _, recv in self.poller.poll():
                recv()
        logger.debug("Node terminating")

    def recv_api(self):
        request = self.pipe.recv_multipart()
        command = request[0].decode('UTF-8')
        if command == "REGISTER":
            service = request[1].decode('UTF-8')
            added = pickle.loads(request[2])
            removed = pickle.loads(request[3])

            ports = self.services[service]
            ports |= added
            ports -= removed

            self.actor.send_unicode("SHOUT", flags=zmq.SNDMORE)
            self.actor.send_unicode(service, flags=zmq.SNDMORE)
            self.actor.send(discovery.Msg.serialize(discovery.Update(ports=ports)))
        elif command == "PEERS":
            service = request[1].decode('UTF-8')
            endpoints = {peer.copy(service=service)
                         for peer in self.peers.values() if len(peer.services[service]) > 0}
            self.pipe.send_pyobj(endpoints)
        else:
            if command == "$TERM":
                for socket in list(self.poller.sockets):
                    self.poller.unregister(socket)
            self.actor.send_multipart(request)

    def recv_backend_enter(self, uuid, name, headers, address):
        name = name.decode('utf-8')
        address = address.decode('utf-8')

        self.add_peer(name, uuid, address)

    def recv_backend_exit(self, uuid, name):
        name = name.decode('utf-8')

        del self.peers[uuid]

    def recv_backend_join(self, uuid, name, group):
        name = name.decode('utf-8')
        group = group.decode('utf-8')

        peer = self.peers[uuid]
        peer.services[group] = set()

    def recv_backend_leave(self, uuid, name, group):
        name = name.decode('utf-8')
        group = group.decode('utf-8')

        peer = self.peers[uuid]
        del peer.services[group]

    def recv_backend_shout(self, uuid, name, group, payload):
        name = name.decode('utf-8')
        group = group.decode('utf-8')

        peer = self.peers[uuid]
        message = discovery.Msg.parse(payload)
        service = message.service or group

        if isinstance(message, discovery.Request):
            peer.update_service(service)
        elif isinstance(message, discovery.Update):
            peer.services[service] = {"{}:{}".format(peer.host_address, port) for port in message.ports}

    def recv_backend_whisper(self, uuid, name, payload):
        name = name.decode('utf-8')

        peer = self.peers[uuid]
        message = discovery.Msg.parse(payload)
        service = message.service

        if isinstance(message, discovery.Request):
            peer.update_service(service)
        elif isinstance(message, discovery.Update):
            peer.services[service] = {"{}:{}".format(peer.host_address, port) for port in message.ports}

    def recv_backend_term(self):
        self.outbox.send_unicode("$TERM")

    def add_peer(self, name, uuid, address):
        peer = NodeActor.Peer(self, name, uuid, address)
        self.peers[uuid] = peer
        return peer


class Node(Pyre):
    def __init__(self, name=None, ctx=None, *args, **kwargs):
        super().__init__(name, ctx, *args, **kwargs)
        backend = self.inbox
        self.inbox, self._outbox = zhelper.zcreate_pipe(self._ctx)

        self.queue = []
        self.actor = ZActor(ctx, NodeActor, self.queue, self.actor, self._outbox, backend)

    def add_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        self.actor.send_unicode("REGISTER", zmq.SNDMORE)
        self.actor.send_unicode(service, zmq.SNDMORE)
        self.actor.send_pyobj({port}, zmq.SNDMORE)
        self.actor.send_pyobj(set())

    def remove_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        self.actor.send_unicode("REGISTER", zmq.SNDMORE)
        self.actor.send_unicode(service, zmq.SNDMORE)
        self.actor.send_pyobj(set(), zmq.SNDMORE)
        self.actor.send_pyobj({port})

    def get_peers(self, service):
        self.actor.send_unicode("PEERS", zmq.SNDMORE)
        self.actor.send_unicode(service)
        return self.actor.recv_pyobj()

    def request_service(self, service):
        self.shout(service, discovery.Msg.serialize(discovery.Request()))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

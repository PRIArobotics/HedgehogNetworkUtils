from collections import defaultdict

import zmq
import logging

from .node import Node
from .. import discovery
from ..zmq.actor import Actor, CommandRegistry
from ..zmq.poller import Poller

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


class ServiceNodeActor(object):
    class Peer:
        def __init__(self, actor, name, uuid, address):
            self.actor = actor
            self.node_actor = self.actor.node_actor
            self.name = name
            self.uuid = uuid
            self.address = address
            self.host_address = address.rsplit(':', 1)[0]
            self.services = defaultdict(set)

        def copy(self):
            result = ServiceNodeActor.Peer(self.actor, self.name, self.uuid, self.address)
            result.actor = None
            result.node_actor = None
            result.services = {k: v for k, v in self.services.items() if len(v) > 0}
            return result

        def request_service(self, service):
            msg = discovery.Msg.serialize(discovery.Request(service))
            self.node_actor.cmd_pipe.send_multipart((b'WHISPER', self.uuid, msg))

        def update_service(self, service):
            ports = self.actor.services[service]
            msg = discovery.Msg.serialize(discovery.Update(service, ports))
            self.node_actor.cmd_pipe.send_multipart((b'WHISPER', self.uuid, msg))

    def __init__(self, ctx, cmd_pipe, evt_pipe, node_actor):
        self.ctx = ctx
        self.cmd_pipe = cmd_pipe
        self.evt_pipe = evt_pipe
        self.node_actor = node_actor

        self.services = defaultdict(set)
        self.peers = {}

        self.poller = Poller()
        # pipe for API commands from client
        self.register_cmd_pipe()
        # pipe for API responses for forwarded commands from node_actor
        # even if we sent commands to node_actor where we expected responses, that's fine because we would wait for that
        # response before polling the next time - nothing gets lost
        self.poller.register(self.node_actor.cmd_pipe, zmq.POLLIN,
                             lambda: self.cmd_pipe.send_multipart(self.node_actor.cmd_pipe.recv_multipart()))
        # pipe for events from node_actor
        self.register_node()

        self.run()

    def register_cmd_pipe(self):
        registry = CommandRegistry()

        def handle_cmd_pipe():
            msg = self.cmd_pipe.recv_multipart()
            try:
                registry.handle(msg)
            except KeyError:
                self.node_actor.cmd_pipe.send_multipart(msg)

        self.poller.register(self.cmd_pipe, zmq.POLLIN, handle_cmd_pipe)

        @registry.command(b'RESTART BEACON')
        def handle_restart_beacon():
            self.node_actor.cmd_pipe.send(b'START')
            self.node_actor.cmd_pipe.wait()

        @registry.command(b'REGISTER')
        def handle_register(service):
            added, removed = self.cmd_pipe.pop()

            ports = self.services[service.decode()]
            ports |= added
            ports -= removed

            msg = discovery.Msg.serialize(discovery.Update(ports=ports))
            self.node_actor.cmd_pipe.send_multipart((b'SHOUT', service, msg))

        @registry.command(b'PEERS')
        def handle_peers():
            peers = {peer.copy() for peer in self.peers.values()}
            self.cmd_pipe.push(peers)
            self.cmd_pipe.signal()

        @registry.command(b'$TERM')
        def handle_term():
            self.terminate()

    def register_node(self):
        registry = CommandRegistry()

        def handle_node():
            msg = self.node_actor.evt_pipe.recv_multipart()
            try:
                forward = registry.handle(msg)
            except KeyError:
                forward = True
            if forward:
                self.evt_pipe.send_multipart(msg)

        self.poller.register(self.node_actor.evt_pipe, zmq.POLLIN,
                             handle_node)

        def command(command, forward=False):
            def decorator(callback):
                def func(*args, **kwargs):
                    callback(*args, **kwargs)
                    return forward
                registry.register(command, func)
            return decorator

        @command(b'BEACON TERM', forward=True)
        def handle_beacon_term():
            pass

        @command(b'ENTER', forward=True)
        def handle_enter(uuid, name, headers, endpoint):
            self.add_peer(name.decode(), uuid, endpoint.decode())

        @command(b'EXIT', forward=True)
        def handle_exit(uuid, name):
            del self.peers[uuid]

        @command(b'JOIN', forward=True)
        def handle_join(uuid, name, group):
            peer = self.peers[uuid]
            peer.services[group.decode()] = set()

        @command(b'LEAVE', forward=True)
        def handle_leave(uuid, name, group):
            peer = self.peers[uuid]
            del peer.services[group.decode()]

        def handle_message(uuid, payload, group=''):
            peer = self.peers[uuid]
            message = discovery.Msg.parse(payload)
            service = message.service or group

            if isinstance(message, discovery.Request):
                peer.update_service(service)
            elif isinstance(message, discovery.Update):
                peer.services[service] = {"{}:{}".format(peer.host_address, port) for port in message.ports}
                self.evt_pipe.push(peer.copy())
                self.evt_pipe.send(b'UPDATE')

        @command(b'SHOUT')
        def handle_shout(uuid, name, group, payload):
            handle_message(uuid, payload, group.decode())

        @command(b'WHISPER')
        def handle_whisper(uuid, name, payload):
            handle_message(uuid, payload)

        @command(b'$TERM')
        def handle_term():
            self.terminate()

    def terminate(self):
        for socket in list(self.poller.sockets):
            self.poller.unregister(socket)

    def add_peer(self, name, uuid, address):
        peer = ServiceNodeActor.Peer(self, name, uuid, address)
        self.peers[uuid] = peer
        return peer

    def run(self):
        # Signal actor successfully initialized
        self.evt_pipe.signal()

        while len(self.poller.sockets) > 0:
            for _, _, handler in self.poller.poll():
                handler()
        logger.debug("Node terminating")


class ServiceNode(Node):
    service_node_class = ServiceNodeActor

    def __init__(self, ctx, name=None):
        super().__init__(ctx, name)

    def start(self):
        super().start()
        node_actor = self.actor
        self.actor = Actor(self.ctx, self.service_node_class, node_actor)
        self.actor._node_actor = node_actor

    def restart_beacon(self):
        self.cmd_pipe.send(b'RESTART BEACON')

    def add_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        self.cmd_pipe.push(({port}, set()))
        self.cmd_pipe.send_multipart((b'REGISTER', service.encode()))

    def remove_service(self, service, endpoint):
        port = endpoint_to_port(endpoint)
        self.cmd_pipe.push((set(), {port}))
        self.cmd_pipe.send_multipart((b'REGISTER', service.encode()))

    def get_peers(self):
        self.cmd_pipe.send(b'PEERS')
        self.cmd_pipe.wait()
        return self.cmd_pipe.pop()

    def request_service(self, service):
        self.shout(service, discovery.Msg.serialize(discovery.Request()))

    def stop(self):
        if self.actor is not None:
            self.actor.destroy()
            self.actor = self.actor._node_actor
        super().stop()

import logging
import socket
import struct
import sys
import time
import uuid
import zmq

from zmq.utils import jsonapi
from pyre.pyre_group import PyreGroup
from pyre.pyre_peer import PyrePeer
from pyre.zre_msg import ZreMsg

from ..zmq import Active
from ..zmq.actor import Actor, CommandRegistry
from ..zmq.poller import Poller

from .beacon import Beacon, BeaconActor

BEACON_VERSION = 1
ZRE_DISCOVERY_PORT = 5670
REAP_INTERVAL = 1.0

logger = logging.getLogger(__name__)


class NodeActor(object):
    def __init__(self, ctx, cmd_pipe, evt_pipe, beacon_class=BeaconActor):
        self.ctx = ctx
        self.cmd_pipe = cmd_pipe
        self.evt_pipe = evt_pipe
        self.beacon_class = beacon_class

        self.verbose = False                        # Log all traffic
        self.beacon_port = ZRE_DISCOVERY_PORT       # Beacon port number
        self.interval = 0                           # Beacon interval 0=default
        self.beacon = None                          # Beacon actor
        self.identity = uuid.uuid4()                # Our UUID as object
        self.inbox = ctx.socket(zmq.ROUTER)         # Our inbox socket (ROUTER)
        try:
            self.inbox.setsockopt(zmq.ROUTER_HANDOVER, 1)
        except AttributeError as e:
            logging.warning("can't set ROUTER_HANDOVER, needs zmq version >=4.1 but installed is {0}".format(zmq.zmq_version()))
        self.name = str(self.identity)[:6]          # Our public name (default=first 6 uuid chars)
        self.endpoint = ""                          # Our public endpoint
        self.port = 0                               # Our inbox port, if any
        self.status = 0                             # Our own change counter
        self.peers = {}                             # Hash of known peers, fast lookup
        self.peer_groups = {}                       # Groups that our peers are in
        self.own_groups = {}                        # Groups that we are in
        self.headers = {}                           # Our header values

        self.poller = Poller()
        self.register_cmd_pipe()

        self.run()

    def start_beacon(self):
        # TODO: If application didn't bind explicitly, we grab an ephemeral port
        # on all available network interfaces. This is orthogonal to
        # beaconing, since we can connect to other peers and they will
        # gossip our endpoint to others.
        if self.beacon_port and not self.beacon:
            # Start beacon discovery
            self.beacon = Beacon(self.ctx)
            self.beacon.beacon_class = self.beacon_class
            self.beacon.start()

            if self.verbose:
                self.beacon.verbose()

            # Our hostname is provided by zbeacon
            hostname = self.beacon.configure(self.beacon_port)

            #if self.interval:
            #   self.beacon.set_interval(self.interval)

            # Our hostname is provided by zbeacon
            self.port = self.inbox.bind_to_random_port("tcp://*")
            if self.port < 0:
                # Die on bad interface or port exhaustion
                logging.critical("Random port assignment for incoming messages failed. Exiting.")
                sys.exit(-1)
            self.endpoint = "tcp://%s:%d" % (hostname, self.port)

            # construct the header filter  (to discard none zre messages)
            self.beacon.subscribe(struct.pack("ccc", b'Z', b'R', b'E'))
            # Set broadcast/listen beacon
            self.beacon.publish(struct.pack('cccb16sH', b'Z', b'R', b'E',
                                            BEACON_VERSION, self.identity.bytes,
                                            socket.htons(self.port)))

            self.register_beacon()
        #else:
        # TODO: gossip stuff

    def start_inbox(self):
        if self.inbox not in self.poller.sockets:
            # Start polling on inbox
            self.register_inbox()
            #logger.debug("Node identity: {0}".format(self.identity))

    def stop_beacon(self):
        logger.debug("Pyre node: stopping beacon")
        if self.beacon:
            self.beacon.publish(struct.pack('cccb16sH', b'Z',b'R',b'E',
                                            BEACON_VERSION, self.identity.bytes,
                                            socket.htons(0)))
            # Give time for beacon to go out
            time.sleep(0.001)
            self.poller.unregister(self.beacon.evt_pipe)
            self.beacon.stop()
            self.beacon = None

        # self.beacon_port = 0

    def stop_inbox(self):
        # Stop polling on inbox
        if self.inbox in self.poller.sockets:
            self.poller.unregister(self.inbox)

        # self.outbox.send_unicode("STOP", zmq.SNDMORE)
        # self.outbox.send(self.identity.bytes, zmq.SNDMORE)
        # self.outbox.send_unicode(self.name)

    def terminate(self):
        self.stop_beacon()
        self.stop_inbox()
        for socket in list(self.poller.sockets):
            self.poller.unregister(socket)

    def register_cmd_pipe(self):
        registry = CommandRegistry()
        self.poller.register(self.cmd_pipe, zmq.POLLIN,
                             lambda: registry.handle(self.cmd_pipe.recv_multipart()))

        @registry.command(b'UUID')
        def handle_uuid():
            self.cmd_pipe.send(self.identity.bytes)

        @registry.command(b'NAME')
        def handle_name():
            self.cmd_pipe.send_unicode(self.name)

        @registry.command(b'SET NAME')
        def handle_set_name(name):
            self.name = name.decode()

        @registry.command(b'SET HEADER')
        def handle_set_header(name, value):
            self.headers.update({name.decode(): value.decode()})

        @registry.command(b'SET VERBOSE')
        def handle_set_verbose():
            self.verbose = True

        @registry.command(b'SET PORT')
        def handle_set_port(port):
            self.beacon_port = int(port)

        @registry.command(b'SET INTERVAL')
        def handle_set_port(interval):
            self.interval = int(interval)

        @registry.command(b'SET ENDPOINT')
        def handle_set_endpoint(*args):
            # TODO: gossip start and endpoint setting
            pass

        @registry.command(b'BIND')
        def handle_bind(endpoint):
            # TODO: Needs a wait-signal
            # self.bind(endpoint.decode())
            pass

        @registry.command(b'CONNECT')
        def handle_set_port(endpoint):
            # TODO: Needs a wait-signal
            # self.connect(endpoint.decode())
            pass

        @registry.command(b'START')
        def handle_start():
            self.start_beacon()
            self.start_inbox()
            self.cmd_pipe.signal()

        @registry.command(b'STOP')
        def handle_stop():
            self.stop_beacon()
            self.stop_inbox()
            self.cmd_pipe.signal()

        @registry.command(b'WHISPER')
        def handle_whisper(id, *content):
            # Get peer to send message to
            id = uuid.UUID(bytes=id)
            peer = self.peers.get(id)
            # Send frame on out to peer's mailbox, drop message
            # if peer doesn't exist (may have been destroyed)
            if peer:
                msg = ZreMsg(ZreMsg.WHISPER)
                msg.set_address(id)
                msg.content = list(content)
                peer.send(msg)

        @registry.command(b'SHOUT')
        def handle_shout(grpname, *content):
            # Get group to send message to
            grpname = grpname.decode()
            group = self.peer_groups.get(grpname)

            if group:
                msg = ZreMsg(ZreMsg.SHOUT)
                msg.set_group(grpname)
                msg.content = list(content)
                self.peer_groups[grpname].send(msg)
            else:
                logger.warning("Group {0} not found.".format(grpname))

        @registry.command(b'JOIN')
        def handle_join(grpname):
            grpname = grpname.decode()
            if not self.own_groups.get(grpname):
                # Only send if we're not already in group
                self.own_groups[grpname] = PyreGroup(grpname)
                msg = ZreMsg(ZreMsg.JOIN)
                msg.set_group(grpname)
                self.status += 1
                msg.set_status(self.status)

                for peer in self.peers.values():
                    peer.send(msg)

                logger.debug("Node is joining group {0}".format(grpname))

        @registry.command(b'LEAVE')
        def handle_leave(grpname):
            grpname = grpname.decode()
            if self.own_groups.get(grpname):
                # Only send if we're actually in group
                msg = ZreMsg(ZreMsg.LEAVE)
                msg.set_group(grpname)
                self.status += 1
                msg.set_status(self.status)

                for peer in self.peers.values():
                    peer.send(msg)

                self.own_groups.pop(grpname)

                logger.debug("Node is leaving group {0}".format(grpname))

        @registry.command(b'PEERS')
        def handle_peers():
            self.cmd_pipe.send_pyobj(list(self.peers.keys()))

        @registry.command(b'PEER NAME')
        def handle_peer_name(id):
            peer = self.peers.get(uuid.UUID(bytes=id))
            if peer:
                self.cmd_pipe.send_unicode("%s" % peer.get_name())
            else:
                self.cmd_pipe.send_unicode("")

        @registry.command(b'PEER ENDPOINT')
        def handle_peer_endpoint(id):
            peer = self.peers.get(uuid.UUID(bytes=id))
            if peer:
                self.cmd_pipe.send_unicode("%s" % peer.get_endpoint())
            else:
                self.cmd_pipe.send_unicode("")

        @registry.command(b'PEER HEADER')
        def handle_peer_header(id, key):
            peer = self.peers.get(uuid.UUID(bytes=id))
            key = key.decode()
            if peer:
                self.cmd_pipe.send_unicode(peer.get_header(key))
            else:
                self.cmd_pipe.send_unicode("")

        @registry.command(b'PEER HEADERS')
        def handle_peer_headers(id):
            peer = self.peers.get(uuid.UUID(bytes=id))
            if peer:
                self.cmd_pipe.send_pyobj(peer.get_headers())
            else:
                self.cmd_pipe.send_unicode("")

        @registry.command(b'PEER GROUPS')
        def handle_peer_groups():
            self.cmd_pipe.send_pyobj(list(self.peer_groups.keys()))

        @registry.command(b'OWN GROUPS')
        def handle_own_groups():
            self.cmd_pipe.send_pyobj(list(self.own_groups.keys()))

        @registry.command(b'DUMP')
        def handle_dump():
            # TODO: zyre_node_dump (self);
            pass

        @registry.command(b'$TERM')
        def handle_term():
            self.terminate()

    def register_beacon(self):
        registry = CommandRegistry()
        self.poller.register(self.beacon.evt_pipe, zmq.POLLIN,
                             lambda: registry.handle(self.beacon.evt_pipe.recv_multipart()))

        @registry.command(b'$TERM')
        def handle_term():
            self.poller.unregister(self.beacon.evt_pipe)
            self.beacon = None
            self.evt_pipe.send(b'BEACON TERM')

        @registry.command(b'BEACON')
        def handle_beacon(ipaddress, beacon):
            ipaddress = ipaddress.decode()
            beacon = struct.unpack('cccb16sH', beacon)

            # Ignore anything that isn't a valid beacon
            if beacon[3] != BEACON_VERSION:
                logger.warning("Invalid ZRE Beacon version: {0}".format(beacon[3]))
                return

            peer_id = uuid.UUID(bytes=beacon[4])
            # print("peerId: %s", peer_id)
            port = socket.ntohs(beacon[5])
            # if we receive a beacon with port 0 this means the peer exited
            if port:
                endpoint = "tcp://%s:%d" % (ipaddress, port)
                peer = self.require_peer(peer_id, endpoint)
                peer.refresh()
            else:
                # Zero port means peer is going away; remove it if
                # we had any knowledge of it already
                peer = self.peers.get(peer_id)
                # remove the peer (delete)
                if peer:
                    logger.debug("Received 0 port beacon, removing peer {0}".format(peer))
                    self.remove_peer(peer)
                else:
                    logger.warning(self.peers)
                    logger.warning("We don't know peer id {0}".format(peer_id))

    def register_inbox(self):
        handlers = {}

        def recv_inbox():
            zmsg = ZreMsg()
            zmsg.recv(self.inbox)
            # msgs = self.inbox.recv_multipart()
            # Router socket tells us the identity of this peer
            # First frame is sender identity
            id = zmsg.get_address()
            # On HELLO we may create the peer if it's unknown
            # On other commands the peer must already exist
            peer = self.peers.get(id)
            if zmsg.id == ZreMsg.HELLO:
                if peer:
                    # remove fake peers
                    if peer.get_ready():
                        self.remove_peer(peer)
                    elif peer.endpoint == self.endpoint:
                        # We ignore HELLO, if peer has same endpoint as current node
                        return

                peer = self.require_peer(id, zmsg.get_endpoint())
                peer.set_ready(True)

            # Ignore command if peer isn't ready
            if not peer or not peer.get_ready():
                logger.warning("Peer {0} isn't ready".format(peer))
                return

            if peer.messages_lost(zmsg):
                logger.warning("{0} messages lost from {1}".format(self.identity, peer.identity))
                self.remove_peer(peer)
                return

            # Now process each command
            handlers[zmsg.id](peer, zmsg)
            # Activity from peer resets peer timers
            peer.refresh()

        self.poller.register(self.inbox, zmq.POLLIN, recv_inbox)

        def command(cmd):
            return lambda func: handlers.update({cmd: func})

        @command(ZreMsg.HELLO)
        def handle_hello(peer, zmsg):
            # Store properties from HELLO command into peer
            peer.set_name(zmsg.get_name())
            peer.set_headers(zmsg.get_headers())

            # Now tell the caller about the peer
            id = peer.get_identity().bytes
            name = peer.get_name()
            headers = jsonapi.dumps(peer.get_headers())
            endpoint = peer.get_endpoint()
            self.evt_pipe.send_multipart((b'ENTER', id, name.encode(), headers, endpoint.encode()))
            logger.debug("({0}) ENTER name={1} endpoint={2}".format(self.name, name, endpoint))

            # Join peer to listed groups
            for grp in zmsg.get_groups():
                self.join_peer_group(peer, grp)
            # Now take peer's status from HELLO, after joining groups
            peer.set_status(zmsg.get_status())

        @command(ZreMsg.WHISPER)
        def handle_whisper(peer, zmsg):
            # Pass up to caller API as WHISPER event
            id = peer.get_identity().bytes
            name = peer.get_name()
            self.evt_pipe.send_multipart((b'WHISPER', id, name.encode()) + tuple(zmsg.content))

        @command(ZreMsg.SHOUT)
        def handle_shout(peer, zmsg):
            # Pass up to caller API as WHISPER event
            id = peer.get_identity().bytes
            name = peer.get_name()
            self.evt_pipe.send_multipart((b'SHOUT', id, name.encode(), zmsg.get_group().encode()) + tuple(zmsg.content))

        @command(ZreMsg.PING)
        def handle_ping(peer, zmsg):
            peer.send(ZreMsg(id=ZreMsg.PING_OK))

        @command(ZreMsg.JOIN)
        def handle_join(peer, zmsg):
            self.join_peer_group(peer, zmsg.get_group())
            assert zmsg.get_status() == peer.get_status()

        @command(ZreMsg.LEAVE)
        def handle_leave(peer, zmsg):
            self.leave_peer_group(peer, zmsg.get_group())
            assert zmsg.get_status() == peer.get_status()

    def purge_peer(self, peer, endpoint):
        if peer.get_endpoint() == endpoint:
            peer.disconnect()
            logger.debug("Purge peer: {0}{1}".format(peer, endpoint))

    # Find or create peer via its UUID string
    def require_peer(self, identity, endpoint):
        p = self.peers.get(identity)
        if not p:
            # Purge any previous peer on same endpoint
            for peer_id, peer in self.peers.copy().items():
                self.purge_peer(peer, endpoint)

            p = PyrePeer(self.ctx, identity)
            self.peers[identity] = p
            p.set_origin(self.name)
            # TODO: this could be handy, to set verbosity on a specific peer
            # zyre_peer_set_verbose (peer, self->verbose);
            p.connect(self.identity, endpoint)

            # Handshake discovery by sending HELLO as first message
            m = ZreMsg(ZreMsg.HELLO)
            m.set_endpoint(self.endpoint)
            m.set_groups(self.own_groups.keys())
            m.set_status(self.status)
            m.set_name(self.name)
            m.set_headers(self.headers)
            p.send(m)

        return p

    #  Remove peer from group, if it's a member
    def delete_peer(self, peer, group):
        group.leave(peer)

    #  Remove a peer from our data structures
    def remove_peer(self, peer):
        # Tell the calling application the peer has gone
        id = peer.get_identity().bytes
        name = peer.get_name()
        self.evt_pipe.send_multipart((b'EXIT', id, name.encode()))
        logger.debug("({0}) EXIT name={1}".format(peer, peer.get_endpoint()))
        # Remove peer from any groups we've got it in
        for grp in self.peer_groups.values():
            self.delete_peer(peer, grp)
        # To destroy peer, we remove from peers hash table (dict)
        self.peers.pop(peer.get_identity())

    # Find or create group via its name
    def require_peer_group(self, groupname):
        grp = self.peer_groups.get(groupname)
        if not grp:
            # somehow a dict containing peers is passed if
            # I don't force the peers arg to an empty dict
            grp = PyreGroup(groupname, peers={})
            self.peer_groups[groupname] = grp

        return grp

    def join_peer_group(self, peer, groupname):
        grp = self.require_peer_group(groupname)
        grp.join(peer)
        # Now tell the caller about the peer joined group
        id = peer.get_identity().bytes
        name = peer.get_name()
        self.evt_pipe.send_multipart((b'JOIN', id, name.encode(), groupname.encode()))
        logger.debug("({0}) JOIN name={1} group={2}".format(self.name, peer.get_name(), groupname))
        return grp

    def leave_peer_group(self, peer, groupname):
        # Tell the caller about the peer joined group
        id = peer.get_identity().bytes
        name = peer.get_name()
        self.evt_pipe.send_multipart((b'LEAVE', id, name.encode(), groupname.encode()))
        # Now remove the peer from the group
        grp = self.require_peer_group(groupname)
        grp.leave(peer)
        logger.debug("({0}) LEAVE name={1} group={2}".format(self.name, peer.get_name(), groupname))

    # We do this once a second:
    # - if peer has gone quiet, send TCP ping
    # - if peer has disappeared, expire it
    def ping_peer(self, peer_id):
        peer = self.peers.get(peer_id)
        if time.time() > peer.expired_at:
            logger.debug("({0}) peer expired name={1} endpoint={2}".format(self.name, peer.get_name(), peer.get_endpoint()))
            self.remove_peer(peer)
        elif time.time() > peer.evasive_at:
            # If peer is being evasive, force a TCP ping.
            # TODO: do this only once for a peer in this state;
            # it would be nicer to use a proper state machine
            # for peer management.
            logger.debug("({0}) peer seems dead/slow name={1} endpoint={2}".format(self.name, peer.get_name(), peer.get_endpoint()))
            msg = ZreMsg(ZreMsg.PING)
            peer.send(msg)

    def run(self):
        # Signal actor successfully initialized
        self.evt_pipe.signal()

        reap_at = time.time() + REAP_INTERVAL
        while len(self.poller.sockets) > 0:
            timeout = reap_at - time.time()
            if timeout < 0:
                timeout = 0

            for _, _, handler in self.poller.poll(timeout * 1000):
                handler()

            if time.time() >= reap_at:
                reap_at = time.time() + REAP_INTERVAL
                # Ping all peers and reap any expired ones
                for peer_id in self.peers.copy().keys():
                    self.ping_peer(peer_id)


class Node(Active):
    node_class = NodeActor
    beacon_class = BeaconActor

    def __init__(self, ctx, name=None):
        self.ctx = ctx
        self.actor = None
        self._uuid = None
        self._name = self._given_name = name

    @property
    def uuid(self):
        if not self._uuid and self.actor:
            self.cmd_pipe.send(b'UUID')
            self._uuid = uuid.UUID(bytes=self.cmd_pipe.recv())
        return self._uuid

    @property
    def name(self):
        if not self._name and self.actor:
            self.cmd_pipe.send(b'NAME')
            self._name = self.cmd_pipe.recv_unicode()
        return self._name

    @name.setter
    def name(self, value):
        self._name = self._given_name = value
        if self.actor:
            self.cmd_pipe.send_multipart((b'SET NAME', value.encode()))

    def start(self):
        self.actor = Actor(self.ctx, self.node_class, beacon_class=self.beacon_class)
        if self._given_name:
            self.cmd_pipe.send_multipart((b'SET NAME', self._given_name.encode()))
        self.cmd_pipe.send(b'START')
        # the backend will signal back
        self.cmd_pipe.wait()

    @property
    def cmd_pipe(self):
        return self.actor.cmd_pipe

    @property
    def evt_pipe(self):
        return self.actor.evt_pipe

    def join(self, group):
        """Join a named group; after joining a group you can send messages to
        the group and all Zyre nodes in that group will receive them."""
        self.cmd_pipe.send_multipart((b'JOIN', group.encode()))

    def leave(self, group):
        """Leave a group"""
        self.cmd_pipe.send_multipart((b'LEAVE', group.encode()))

    # Send message to single peer; peer ID is first frame in message
    def whisper(self, peer, msg_p):
        """Send message to single peer, specified as a UUID string
        Destroys message after sending"""
        msg_p = tuple(msg_p) if isinstance(msg_p, list) else (msg_p,)
        payload = (b'WHISPER', peer.bytes) + msg_p
        self.cmd_pipe.send_multipart(payload)

    def shout(self, group, msg_p):
        """Send message to a named group
        Destroys message after sending"""
        msg_p = tuple(msg_p) if isinstance(msg_p, list) else (msg_p,)
        payload = (b'SHOUT', group.encode()) + msg_p
        self.cmd_pipe.send_multipart(payload)

    def stop(self):
        if self.actor is not None:
            self.cmd_pipe.send(b'STOP')
            # the backend will signal back
            self.cmd_pipe.wait()
            self.actor.destroy()
            self.actor = None

            self._uuid = None
            self._name = self._given_name
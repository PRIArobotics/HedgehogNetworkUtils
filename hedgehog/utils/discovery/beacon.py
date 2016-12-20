import logging
import ipaddress
import socket
import struct
import time
import zmq

from contextlib import suppress
from sys import platform
from pyre import zhelper
from ..zmq import Active
from ..zmq.actor import Actor, CommandRegistry
from ..zmq.poller import Poller

logger = logging.getLogger(__name__)

INTERVAL_DFLT = 1.0
BEACON_MAX = 255      # Max size of beacon data
MULTICAST_GRP = '225.25.25.25'


class BeaconActor(object):
    def __init__(self, ctx, cmd_pipe, evt_pipe):
        self.ctx = ctx
        self.cmd_pipe = cmd_pipe
        self.evt_pipe = evt_pipe

        self.udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.port_nbr = 0
        self.interval = INTERVAL_DFLT
        self.ping_at = 0
        self.transmit = None
        self.filter = b""

        self.verbose = False
        self.hostname = ""

        self.address = None
        self.network_address = None
        self.broadcast_address = None
        self.interface_name = None

        self.poller = Poller()
        self.register_cmd_pipe()
        self.poller.register(self.udpsock.fileno(), zmq.POLLIN, self.handle_udpsock)

        self.run()

    def prepare_udp(self):
        self._prepare_socket()
        try:
            self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            #  On some platforms we have to ask to reuse the port
            with suppress(AttributeError):
                self.udpsock.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_REUSEPORT, 1)

            if self.broadcast_address.is_multicast:
                # TTL
                self.udpsock.setsockopt(socket.IPPROTO_IP,
                                        socket.IP_MULTICAST_TTL, 2)

                # TODO: This should only be used if we do not have inproc method!
                self.udpsock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

                # Usually, the system administrator specifies the
                # default interface multicast datagrams should be
                # sent from. The programmer can override this and
                # choose a concrete outgoing interface for a given
                # socket with this option.
                #
                # this results in the loopback address?
                # host = socket.gethostbyname(socket.gethostname())
                # self.udpsock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(host))
                # You need to tell the kernel which multicast groups
                # you are interested in. If no process is interested
                # in a group, packets destined to it that arrive to
                # the host are discarded.
                # You can always fill this last member with the
                # wildcard address (INADDR_ANY) and then the kernel
                # will deal with the task of choosing the interface.
                #
                # Maximum memberships: /proc/sys/net/ipv4/igmp_max_memberships
                # self.udpsock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                #       socket.inet_aton("225.25.25.25") + socket.inet_aton(host))
                self.udpsock.bind(("", self.port_nbr))

                group = socket.inet_aton("{0}".format(self.broadcast_address))
                mreq = struct.pack('4sl', group, socket.INADDR_ANY)

                self.udpsock.setsockopt(socket.SOL_IP,
                                        socket.IP_ADD_MEMBERSHIP, mreq)

            else:
                # Platform specifics
                if platform.startswith("linux"):
                    # on linux we bind to the broadcast address and send to
                    # the broadcast address
                    self.udpsock.bind((str(self.broadcast_address),
                                       self.port_nbr))
                else:
                    self.udpsock.bind(("", self.port_nbr))

                logger.debug("Set up a broadcast beacon to {0}:{1}".format(self.broadcast_address, self.port_nbr))
        except socket.error:
            logger.exception("Initializing of {0} raised an exception".format(self.__class__.__name__))

    def _prepare_socket(self):
        # TODO this may return None for no apparent reason (file descriptors exhausted maybe?), non-recoverable
        netinf = zhelper.get_ifaddrs()

        logger.debug("Available interfaces: {0}".format(netinf))

        for iface in netinf:
            # Loop over the interfaces and their settings to try to find the broadcast address.
            # ipv4 only currently and needs a valid broadcast address
            for name, data in iface.items():
                logger.debug("Checking out interface {0}.".format(name))
                # For some reason the data we need lives in the "2" section of the interface.
                data_2 = data.get(2)

                if not data_2:
                    logger.debug("No data_2 found for interface {0}.".format(name))
                    continue

                address_str = data_2.get("addr")
                netmask_str = data_2.get("netmask")

                if not address_str or not netmask_str:
                    logger.debug("Address or netmask not found for interface {0}.".format(name))
                    continue

                if isinstance(address_str, bytes):
                    address_str = address_str.decode("utf8")

                if isinstance(netmask_str, bytes):
                    netmask_str = netmask_str.decode("utf8")

                interface_string = "{0}/{1}".format(address_str, netmask_str)

                interface = ipaddress.ip_interface(zhelper.u(interface_string))

                if interface.is_loopback:
                    logger.debug("Interface {0} is a loopback device.".format(name))
                    continue

                if interface.is_link_local:
                    logger.debug("Interface {0} is a link-local device.".format(name))
                    continue

                self.address = interface.ip
                self.network_address = interface.network.network_address
                self.broadcast_address = interface.network.broadcast_address
                self.interface_name = name

            if self.address:
                break

        logger.debug("Finished scanning interfaces.")

        if not self.address:
            self.network_address = ipaddress.IPv4Address(zhelper.u('127.0.0.1'))
            self.broadcast_address = ipaddress.IPv4Address(zhelper.u(MULTICAST_GRP))
            self.interface_name = 'loopback'
            self.address = zhelper.u('127.0.0.1')

        logger.debug("Address: {0}".format(self.address))
        logger.debug("Network: {0}".format(self.network_address))
        logger.debug("Broadcast: {0}".format(self.broadcast_address))
        logger.debug("Interface name: {0}".format(self.interface_name))

    def configure(self, port_nbr):
        self.port_nbr = port_nbr
        self.prepare_udp()

    def register_cmd_pipe(self):
        registry = CommandRegistry()
        self.poller.register(self.cmd_pipe, zmq.POLLIN,
                             lambda: registry.handle(self.cmd_pipe.recv_multipart()))

        @registry.command(b'VERBOSE')
        def handle_verbose():
            self.verbose = True

        @registry.command(b'CONFIGURE')
        def handle_configure(port):
            port = struct.unpack('I', port)[0]
            self.configure(port)
            self.cmd_pipe.send_unicode(str(self.address))

        @registry.command(b'PUBLISH')
        def handle_publish(transmit):
            self.transmit = transmit
            if self.interval == 0:
                self.interval = INTERVAL_DFLT
            # Start broadcasting immediately
            self.ping_at = time.time()

        @registry.command(b'SILENCE')
        def handle_silence():
            self.transmit = None

        @registry.command(b'SUBSCRIBE')
        def handle_subscribe(filter):
            self.filter = filter

        @registry.command(b'UNSUBSCRIBE')
        def handle_unsubscribe():
            self.filter = None

        @registry.command(b'$TERM')
        def handle_term():
            self.terminate()

    def terminate(self):
        for socket in list(self.poller.sockets):
            self.poller.unregister(socket)

    def handle_udpsock(self):
        try:
            frame, addr = self.udpsock.recvfrom(BEACON_MAX)

        except Exception as e:
            logger.exception("Exception while receiving: {0}".format(e))
            return

        peername = addr[0]

        #  If filter is set, check that beacon matches it
        is_valid = False
        if self.filter is not None and len(self.filter) <= len(frame):
            match_data = frame[:len(self.filter)]
            if match_data == self.filter:
                is_valid = True

        #  If valid, discard our own broadcasts, which UDP echoes to us
        if is_valid and self.transmit:
            if frame == self.transmit:
                is_valid = False

        #  If still a valid beacon, send on to the API
        if is_valid:
            self.evt_pipe.send_multipart((b'BEACON', peername.encode(), frame))

    def send_beacon(self):
        try:
            self.udpsock.sendto(self.transmit, (str(self.broadcast_address),
                                                self.port_nbr))
        except OSError:
            logger.info("Network seems gone, exiting zbeacon")
            self.terminate()

    def run(self):
        # Signal actor successfully initialized
        self.evt_pipe.signal()

        while len(self.poller.sockets) > 0:
            timeout = 1
            if self.transmit:
                timeout = self.ping_at - time.time()
                if timeout < 0:
                    timeout = 0

            for _, _, handler in self.poller.poll(timeout * 1000):
                handler()

            if self.transmit and time.time() >= self.ping_at:
                self.send_beacon()
                self.ping_at = time.time() + self.interval
        self.udpsock.close()


class Beacon(Active):
    beacon_class = BeaconActor

    def __init__(self, ctx):
        self.ctx = ctx
        self.actor = None

    def start(self):
        self.actor = Actor(self.ctx, self.beacon_class)

    @property
    def cmd_pipe(self):
        return self.actor.cmd_pipe

    @property
    def evt_pipe(self):
        return self.actor.evt_pipe

    def verbose(self):
        self.cmd_pipe.send(b'VERBOSE')

    def configure(self, port):
        self.cmd_pipe.send_multipart((b'CONFIGURE', struct.pack("I", port)))
        return self.cmd_pipe.recv_unicode()

    def publish(self, transmit):
        self.cmd_pipe.send_multipart((b'PUBLISH', transmit))

    def subscribe(self, filter):
        self.cmd_pipe.send_multipart((b'SUBSCRIBE', filter))

    def unsubscribe(self):
        self.cmd_pipe.send_multipart(b'UNSUBSCRIBE')

    def stop(self):
        if self.actor is not None:
            self.actor.destroy()
            self.actor = None

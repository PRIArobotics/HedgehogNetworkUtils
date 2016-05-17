import zmq
from pyre import zhelper


def pipe(ctx=None, hwm=1000):
    if ctx is None:
        ctx = zmq.Context.instance()
    return zhelper.zcreate_pipe(ctx, hwm)


class Poller:
    def __init__(self):
        self._poller = zmq.Poller()
        self.data = {}

    @property
    def sockets(self):
        return self.data.keys()

    def __contains__(self, socket):
        return socket in self.data

    def register(self, socket, flags=zmq.POLLIN|zmq.POLLOUT, data=None):
        self._poller.register(socket, flags)
        self.data[socket] = data

    def modify(self, socket, flags=zmq.POLLIN|zmq.POLLOUT, data=None):
        self.register(socket, data, flags)

    def unregister(self, socket):
        self._poller.unregister(socket)
        del self.data[socket]

    def poll(self, timeout=None):
        return [(socket, event, self.data[socket]) for socket, event in self._poller.poll(timeout)]

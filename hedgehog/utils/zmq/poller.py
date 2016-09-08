import zmq


class Poller(object):
    """
    This class wraps the ZMQ `Poller` class to assign an additional data object to each socket.
    Compared to `zmq.Poller`, this class' `register` and `modify` methods take an additional `data` parameter, and the
    `poll` method returns a list consisting of tuples with an additional `data` entry. The data is optional and may be
    arbitrary, but one common use would be a callback that is thus easily associated with each socket.
    """
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
        self.register(socket, flags, data)

    def unregister(self, socket):
        self._poller.unregister(socket)
        del self.data[socket]

    def poll(self, timeout=None):
        return [(socket, event, self.data[socket]) for socket, event in self._poller.poll(timeout)]

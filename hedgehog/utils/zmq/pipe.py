import zmq
from queue import Queue
import random

from .socket import Socket


def __pipe(ctx, endpoint=None, hwm=1000, socket=Socket):
    frontend, backend = (socket(ctx, zmq.PAIR).configure(hwm=hwm, linger=0) for _ in range(2))

    if endpoint is not None:
        frontend.bind(endpoint)
        backend.connect(endpoint)
        return frontend, backend
    else:
        while True:
            endpoint = "inproc://pipe-%04x-%04x" % (random.randint(0, 0x10000), random.randint(0, 0x10000))
            try:
                frontend.bind(endpoint)
                backend.connect(endpoint)
            except zmq.error.ZMQError:
                pass
            else:
                return frontend, backend


def pipe(ctx, endpoint=None, hwm=1000):
    """
    Returns two PAIR sockets frontend, backend that are connected to each other. If no endpoint is given, a random
    (inproc) endpoint will be used for the connection.

    :return: The frontend, backend socket pair
    """
    return __pipe(ctx, endpoint, hwm)


def extended_pipe(ctx, endpoint=None, hwm=1000, maxsize=0):
    """
    Returns two PAIR sockets frontend, backend that are connected to each other. If no endpoint is given, a random
    (inproc) endpoint will be used for the connection. The returned sockets will support additional methods `push` and
    `pop`, which can be used to transfer objects by reference. Example usage:

        a, b = extended_pipe(zmq.Context())

        obj = object()
        a.push(obj)
        a.signal()
        b.wait()
        assert b.pop() is obj

    As you can see, push and pop don't do any socket communication, they merely provide the data in a queue accessible
    to the other end of the pipe. Care must be taken if transferred objects are mutable, as this may introduce race
    conditions if not done properly.

    :return: The frontend, backend socket pair
    """
    class ExtendedSocket(Socket):
        in_queue = None
        out_queue = None

        def push(self, obj):
            self.out_queue.put(obj)

        def pop(self):
            # don't block, as we expect access synchronized via zmq sockets
            return self.in_queue.get(block=False)

    frontend, backend = __pipe(ctx, endpoint, hwm, ExtendedSocket)
    frontend.in_queue = backend.out_queue = Queue(maxsize)
    frontend.out_queue = backend.in_queue = Queue(maxsize)
    return frontend, backend

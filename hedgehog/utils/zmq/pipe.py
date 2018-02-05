from typing import Any, Tuple, Type, TypeVar

import zmq
from queue import Queue
import random

from .socket import Socket


T = TypeVar('T', bound=Socket)


def __pipe(ctx: zmq.Context, endpoint: str=None, hwm: int=1000, socket: Type[T]=Socket) -> Tuple[T, T]:
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
            except zmq.error.ZMQError:  # pragma: no cover
                pass
            else:
                return frontend, backend


def pipe(ctx: zmq.Context, endpoint: str=None, hwm: int=1000) -> Tuple[Socket, Socket]:
    """
    Returns two PAIR sockets frontend, backend that are connected to each other. If no endpoint is given, a random
    (inproc) endpoint will be used for the connection.

    :return: The frontend, backend socket pair
    """
    return __pipe(ctx, endpoint, hwm)


class ExtendedSocket(Socket):
    in_queue = None  # type: Queue
    out_queue = None  # type: Queue

    def push(self, obj: Any) -> None:
        self.out_queue.put(obj)

    def pop(self) -> Any:
        # don't block, as we expect access synchronized via zmq sockets
        return self.in_queue.get(block=False)


def extended_pipe(ctx: zmq.Context, endpoint: str=None, hwm: int=1000, maxsize: int=0) -> Tuple[ExtendedSocket, ExtendedSocket]:
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
    frontend, backend = __pipe(ctx, endpoint, hwm, ExtendedSocket)
    frontend.in_queue = backend.out_queue = Queue(maxsize)
    frontend.out_queue = backend.in_queue = Queue(maxsize)
    return frontend, backend

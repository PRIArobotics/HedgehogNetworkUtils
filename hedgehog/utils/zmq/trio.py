from typing import Tuple, Union

import zmq.asyncio

from trio_asyncio import aio_as_trio

from .asyncio import Socket as _Socket

__all__ = ['Context', 'Socket', 'Fileno', 'SocketLike']


class Socket(_Socket):
    """
    A zmq.Socket subclass that simply adds some convenience functions.
    """

    @aio_as_trio
    def recv_multipart(self, flags=0, copy=True, track=False):
        return super().recv_multipart(flags, copy, track)

    @aio_as_trio
    def recv(self, flags=0, copy=True, track=False):
        return super().recv(flags, copy, track)

    @aio_as_trio
    def send_multipart(self, msg, flags=0, copy=True, track=False, **kwargs):
        return super().send_multipart(msg, flags, copy, track, **kwargs)

    @aio_as_trio
    def send(self, msg, flags=0, copy=True, track=False, **kwargs):
        return super().send(msg, flags, copy, track, **kwargs)

    @aio_as_trio
    def poll(self, timeout=None, flags=zmq.POLLIN):
        return super().poll(timeout, flags)


class Context(zmq.Context):
    _socket_class = Socket


Fileno = int
SocketLike = Union[Socket, Fileno]

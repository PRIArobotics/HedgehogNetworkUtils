from typing import Union

import zmq.asyncio

from . import _ConfigureSocketMixin, _AsyncSocketExtensionsMixin

__all__ = ['Context', 'Socket', 'Fileno', 'SocketLike']


class Socket(_ConfigureSocketMixin, _AsyncSocketExtensionsMixin, zmq.asyncio.Socket):
    """
    A zmq.Socket subclass that simply adds some convenience functions; asyncio version.
    """


class Context(zmq.Context):
    _socket_class = Socket


Fileno = int
SocketLike = Union[Socket, Fileno]

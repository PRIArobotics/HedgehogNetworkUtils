from typing import Tuple, Union

import zmq.asyncio

from itertools import zip_longest


class Socket(zmq.asyncio.Socket):
    """
    A zmq.Socket subclass that simply adds some convenience functions.
    """

    def configure(self, hwm: int=None, rcvtimeo: int=None, sndtimeo: int=None, linger: int=None) -> 'Socket':
        """
        Allows to configure some common socket options and configurations, while allowing method chaining
        """
        if hwm is not None:
            self.set_hwm(hwm)
        if rcvtimeo is not None:
            self.setsockopt(zmq.RCVTIMEO, rcvtimeo)
        if sndtimeo is not None:
            self.setsockopt(zmq.SNDTIMEO, sndtimeo)
        if linger is not None:
            self.setsockopt(zmq.LINGER, linger)
        return self

    async def signal(self) -> None:
        """
        Sends an empty single-frame message, i.e. an event with no data attached.
        """
        await self.send(b'')

    async def wait(self) -> None:
        """
        Waits for the next message and asserts that it contained no data, such as those sent by `signal()`.
        """
        await self.recv_expect(b'')

    async def recv_expect(self, data: bytes=b'') -> None:
        """
        Waits for the next message and asserts that it contains the given data.
        """
        recvd = await self.recv()
        assert recvd == data

    async def recv_multipart_expect(self, data: Tuple[bytes, ...]=(b'',)) -> None:
        """
        Waits for the next multipart message and asserts that it contains the given data.
        """
        recvd = await self.recv_multipart()
        assert all(a == b for a, b in zip_longest(recvd, data))


Fileno = int
SocketLike = Union[Socket, Fileno]

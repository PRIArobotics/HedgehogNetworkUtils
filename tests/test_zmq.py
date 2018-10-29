import pytest
from hedgehog.utils.test_utils import event_loop, zmq_ctx, zmq_aio_ctx, assertTimeout, assertImmediate

import asyncio
import zmq


# Pytest fixtures
event_loop, zmq_ctx, zmq_aio_ctx


class TestAsyncSocket(object):
    @pytest.mark.asyncio
    async def test_async_socket_configure(self, zmq_aio_ctx):
        from hedgehog.utils.zmq.async_socket import Socket

        with Socket(zmq_aio_ctx, zmq.PAIR).configure() as socket:
            assert socket.get_hwm() == 1000
            assert socket.getsockopt(zmq.RCVTIMEO) == -1
            assert socket.getsockopt(zmq.SNDTIMEO) == -1
            assert socket.getsockopt(zmq.LINGER) == -1

            socket.configure(hwm=2000, rcvtimeo=100, sndtimeo=100, linger=0)
            assert socket.get_hwm() == 2000
            assert socket.getsockopt(zmq.RCVTIMEO) == 100
            assert socket.getsockopt(zmq.SNDTIMEO) == 100
            assert socket.getsockopt(zmq.LINGER) == 0

    @pytest.mark.asyncio
    async def test_async_socket(self, zmq_aio_ctx):
        from hedgehog.utils.zmq.async_socket import Socket

        a, b = (Socket(zmq_aio_ctx, zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
        with a, b:
            a.bind('inproc://endpoint')
            b.connect('inproc://endpoint')

            task = asyncio.ensure_future(b.wait())
            await assertTimeout(task, 1, shield=True)
            with assertImmediate():
                await a.signal()
                await task

            task = asyncio.ensure_future(b.recv_multipart_expect((b'foo', b'bar')))
            await assertTimeout(task, 1, shield=True)
            with assertImmediate():
                await a.send_multipart((b'foo', b'bar'))
                await task


class TestSocket(object):
    def test_async_socket_configure(self, zmq_ctx):
        from hedgehog.utils.zmq.socket import Socket

        with Socket(zmq_ctx, zmq.PAIR).configure() as socket:
            assert socket.get_hwm() == 1000
            assert socket.getsockopt(zmq.RCVTIMEO) == -1
            assert socket.getsockopt(zmq.SNDTIMEO) == -1
            assert socket.getsockopt(zmq.LINGER) == -1

            socket.configure(hwm=2000, rcvtimeo=100, sndtimeo=100, linger=0)
            assert socket.get_hwm() == 2000
            assert socket.getsockopt(zmq.RCVTIMEO) == 100
            assert socket.getsockopt(zmq.SNDTIMEO) == 100
            assert socket.getsockopt(zmq.LINGER) == 0

    def test_socket(self, zmq_ctx):
        from hedgehog.utils.zmq.socket import Socket

        a, b = (Socket(zmq_ctx, zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
        with a, b:
            a.bind('inproc://endpoint')
            b.connect('inproc://endpoint')

            poller = zmq.Poller()
            poller.register(b, zmq.POLLIN)

            assert poller.poll(0.01) == []

            a.signal()
            assert poller.poll(0.01) == [(b, zmq.POLLIN)]
            b.wait()

            assert poller.poll(0.01) == []

            a.send_multipart((b'foo', b'bar'))
            assert poller.poll(0.01) == [(b, zmq.POLLIN)]
            b.recv_multipart_expect((b'foo', b'bar'))

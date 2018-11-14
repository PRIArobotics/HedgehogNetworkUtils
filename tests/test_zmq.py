import pytest
from hedgehog.utils.test_utils import zmq_ctx
from hedgehog.utils.test_utils import event_loop, zmq_aio_ctx, assertTimeout, assertPassed
from hedgehog.utils.test_utils import zmq_trio_ctx, assertTimeoutTrio, assertPassedTrio

import asyncio
import trio_asyncio
import trio.testing
import zmq


# Pytest fixtures
event_loop, zmq_ctx, zmq_aio_ctx, zmq_trio_ctx


class TestSocket(object):
    def test_socket_configure(self, zmq_ctx):
        with zmq_ctx.socket(zmq.PAIR).configure() as socket:
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
        a, b = (zmq_ctx.socket(zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
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


class TestAsyncSocket(object):
    @pytest.mark.asyncio
    async def test_async_socket_configure(self, zmq_aio_ctx):
        with zmq_aio_ctx.socket(zmq.PAIR).configure() as socket:
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
        a, b = (zmq_aio_ctx.socket(zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
        with a, b:
            a.bind('inproc://endpoint')
            b.connect('inproc://endpoint')

            with assertPassed(1):
                task = asyncio.ensure_future(b.wait())
                await assertTimeout(task, 1, shield=True)
                await a.signal()
                await task

            with assertPassed(1):
                task = asyncio.ensure_future(b.recv_multipart_expect((b'foo', b'bar')))
                await assertTimeout(task, 1, shield=True)
                await a.send_multipart((b'foo', b'bar'))
                await task


class TestTrioSocket(object):
    @pytest.mark.trio
    async def test_trio_socket_configure(self, zmq_trio_ctx, autojump_clock):
        async with trio_asyncio.open_loop():
            with zmq_trio_ctx.socket(zmq.PAIR).configure() as socket:
                assert socket.get_hwm() == 1000
                assert socket.getsockopt(zmq.RCVTIMEO) == -1
                assert socket.getsockopt(zmq.SNDTIMEO) == -1
                assert socket.getsockopt(zmq.LINGER) == -1

                socket.configure(hwm=2000, rcvtimeo=100, sndtimeo=100, linger=0)
                assert socket.get_hwm() == 2000
                assert socket.getsockopt(zmq.RCVTIMEO) == 100
                assert socket.getsockopt(zmq.SNDTIMEO) == 100
                assert socket.getsockopt(zmq.LINGER) == 0

    @pytest.mark.trio
    async def test_trio_socket(self, zmq_trio_ctx, autojump_clock):
        async with trio_asyncio.open_loop():
            a, b = (zmq_trio_ctx.socket(zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
            with a, b:
                a.bind('inproc://endpoint')
                b.connect('inproc://endpoint')

                with assertTimeoutTrio(1):
                    await b.wait()

                with assertTimeoutTrio(1):
                    await b.poll()

                assert await b.poll(timeout=1) == 0

                seq = trio.testing.Sequencer()
                async with trio.open_nursery() as nursery:
                    @nursery.start_soon
                    async def recv():
                        with assertPassedTrio(1):
                            # nothing is sent yet, this times out
                            async with seq(0):
                                with assertTimeoutTrio(1):
                                    await b.wait()
                            # now the message was sent, we expect to receive immediately
                            async with seq(2):
                                await b.wait()

                    async with seq(1):
                        await a.signal()

                with assertTimeoutTrio(1):
                    await b.recv_multipart_expect((b'foo', b'bar'))

                seq = trio.testing.Sequencer()
                async with trio.open_nursery() as nursery:
                    @nursery.start_soon
                    async def recv():
                        with assertPassedTrio(1):
                            # nothing is sent yet, this times out
                            async with seq(0):
                                with assertTimeoutTrio(1):
                                    await b.recv_multipart_expect((b'foo', b'bar'))
                            # now the message was sent, we expect to receive immediately
                            async with seq(2):
                                await b.recv_multipart_expect((b'foo', b'bar'))

                    async with seq(1):
                        await a.send_multipart((b'foo', b'bar'))

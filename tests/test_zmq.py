try:
    import zmq
except ImportError:
    # testing without the zmq feature
    pass
else:
    import pytest
    from hedgehog.utils.test_utils import zmq_ctx
    from hedgehog.utils.test_utils import event_loop, zmq_aio_ctx, assertTimeout, assertPassed

    import asyncio

    # Pytest fixtures
    event_loop, zmq_ctx, zmq_aio_ctx


    def do_test_socket_configure(socket):
        assert socket.get_hwm() == 1000
        assert socket.getsockopt(zmq.RCVTIMEO) == -1
        assert socket.getsockopt(zmq.SNDTIMEO) == -1
        assert socket.getsockopt(zmq.LINGER) == -1

        socket.configure(hwm=2000, rcvtimeo=100, sndtimeo=100, linger=0)
        assert socket.get_hwm() == 2000
        assert socket.getsockopt(zmq.RCVTIMEO) == 100
        assert socket.getsockopt(zmq.SNDTIMEO) == 100
        assert socket.getsockopt(zmq.LINGER) == 0


    class TestSocket(object):
        def test_socket_configure(self, zmq_ctx):
            with zmq_ctx.socket(zmq.PAIR).configure() as socket:
                do_test_socket_configure(socket)

        def test_socket(self, zmq_ctx):
            a, b = (zmq_ctx.socket(zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
            with a, b:
                a.bind('inproc://endpoint')
                b.connect('inproc://endpoint')

                assert b.poll(0.01) == 0

                a.signal()
                assert b.poll(0.01) == zmq.POLLIN
                b.wait()

                assert b.poll(0.01) == 0

                a.send_multipart((b'foo', b'bar'))
                assert b.poll(0.01) == zmq.POLLIN
                b.recv_multipart_expect((b'foo', b'bar'))


    class TestAsyncSocket(object):
        @pytest.mark.asyncio
        async def test_async_socket_configure(self, zmq_aio_ctx):
            with zmq_aio_ctx.socket(zmq.PAIR).configure() as socket:
                do_test_socket_configure(socket)

        @pytest.mark.asyncio
        async def test_async_socket(self, zmq_aio_ctx):
            a, b = (zmq_aio_ctx.socket(zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
            with a, b:
                a.bind('inproc://endpoint')
                b.connect('inproc://endpoint')

                await assertTimeout(b.poll(), 1)

                assert await b.poll(1) == 0

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


try:
    import zmq
    import trio_asyncio
except ImportError:
    # testing without the zmq or trio feature
    pass
else:
    from hedgehog.utils.test_utils import zmq_trio_ctx, assertTimeoutTrio

    # Pytest fixtures
    zmq_trio_ctx


    class TestTrioSocket(object):
        @pytest.mark.trio
        async def test_trio_socket_configure(self, zmq_trio_ctx, autojump_clock):
            async with trio_asyncio.open_loop():
                with zmq_trio_ctx.socket(zmq.PAIR).configure() as socket:
                    do_test_socket_configure(socket)

        @pytest.mark.trio
        async def test_trio_socket(self, zmq_trio_ctx, autojump_clock):
            async with trio_asyncio.open_loop():
                a, b = (zmq_trio_ctx.socket(zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
                with a, b:
                    a.bind('inproc://endpoint')
                    b.connect('inproc://endpoint')

                    with assertTimeoutTrio(1):
                        await b.poll()

                    assert await b.poll(timeout=1) == 0

                    with assertPassed(1):
                        with assertTimeoutTrio(1):
                            await b.wait()
                        await a.signal()
                        await b.wait()

                    with assertPassed(1):
                        with assertTimeoutTrio(1):
                            await b.recv_multipart_expect((b'foo', b'bar'))
                        await a.send_multipart((b'foo', b'bar'))
                        await b.recv_multipart_expect((b'foo', b'bar'))

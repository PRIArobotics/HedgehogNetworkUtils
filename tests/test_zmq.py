import pytest
from hedgehog.utils.test_utils import event_loop, zmq_ctx, zmq_aio_ctx, assertTimeout, assertImmediate

import asyncio
import zmq

from hedgehog.utils.zmq.async_socket import Socket


# Pytest fixtures
event_loop, zmq_ctx, zmq_aio_ctx


class TestAsyncSocket(object):
    @pytest.mark.asyncio
    async def test_async_socket(self, zmq_aio_ctx):
        a, b = (Socket(zmq_aio_ctx, zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
        with a, b:
            a.bind('inproc://endpoint')
            b.connect('inproc://endpoint')

            task = asyncio.ensure_future(b.wait())
            await assertTimeout(task, 1, shield=True)
            with assertImmediate():
                await a.signal()
                await task


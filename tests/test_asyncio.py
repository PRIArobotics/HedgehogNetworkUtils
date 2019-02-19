import pytest
from hedgehog.utils.test_utils import event_loop, assertImmediate

import asyncio
import itertools
from aiostream import stream, pipe

from hedgehog.utils.asyncio import repeat_func, repeat_func_eof, stream_from_queue


# Pytest fixtures
event_loop


async def assert_stream(expected, _stream):
    async with stream.enumerate(_stream).stream() as streamer:
        i = -1
        async for i, item in streamer:
            assert item == expected[i]
        assert i == len(expected) - 1


@pytest.mark.asyncio
async def test_repeat_func():
    with assertImmediate():
        await assert_stream(
            [0, 1, 2],
            repeat_func(itertools.count().__next__, 3))


@pytest.mark.asyncio
async def test_repeat_func_eof():
    with assertImmediate():
        await assert_stream(
            [0, 1, 2],
            repeat_func_eof(itertools.count().__next__, 3))


@pytest.mark.asyncio
async def test_stream_from_queue():
    with assertImmediate():
        queue = asyncio.Queue()
        await queue.put(0)

        async def put_next(item):
            await queue.put(item + 1)

        await assert_stream(
            [0, 1, 2],
            (stream_from_queue(queue) | pipe.action(put_next))[:3])


@pytest.mark.asyncio
async def test_stream_from_queue_eof():
    with assertImmediate():
        EOF = object()
        queue = asyncio.Queue()
        await queue.put(3)

        async def put_next(item):
            await queue.put(item - 1 if item > 0 else EOF)

        await assert_stream(
            [3, 2, 1, 0],
            stream_from_queue(queue, EOF, use_is=True) | pipe.action(put_next))

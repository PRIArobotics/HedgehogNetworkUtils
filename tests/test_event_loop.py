import pytest

import concurrent.futures
from aiostream.context_utils import async_context_manager
from hedgehog.utils.event_loop import EventLoopThread


def test_event_loop_thread():
    with EventLoopThread():
        pass


def test_call_soon():
    future = concurrent.futures.Future()

    def callback():
        if future.set_running_or_notify_cancel():
            future.set_result(None)

    with EventLoopThread() as loop:
        loop.call_soon(callback)

    assert future.done()


def test_run_coroutine():
    future = concurrent.futures.Future()

    async def coro_fun():
        if future.set_running_or_notify_cancel():
            future.set_result(None)
        return 1

    with EventLoopThread() as loop:
        assert loop.run_coroutine(coro_fun()).result() == 1
        assert future.done()


def test_context():
    enter = concurrent.futures.Future()
    exit = concurrent.futures.Future()

    @async_context_manager
    async def context_manager():
        if enter.set_running_or_notify_cancel():
            enter.set_result(None)
        try:
            yield 1
        finally:
            if exit.set_running_or_notify_cancel():
                exit.set_result(None)

    with EventLoopThread() as loop:
        mgr = context_manager()
        assert not enter.done()
        with loop.context(mgr) as result:
            assert enter.done()
            assert result == 1
            assert not exit.done()
        assert exit.done()


def test_context_raises():
    @async_context_manager
    async def catching():
        with pytest.raises(Exception, message="foo"):
            yield

    @async_context_manager
    async def noncatching():
        yield

    with EventLoopThread() as loop:
        with loop.context(catching()):
            raise Exception("foo")

        with pytest.raises(Exception, message="foo"),\
             loop.context(noncatching()):
            raise Exception("foo")

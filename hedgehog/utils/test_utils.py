from typing import Any, Generator, List

import pytest
import asyncio
import logging
import selectors
import zmq.asyncio
from contextlib import contextmanager


class SelectorTimeTrackingTestLoop(asyncio.SelectorEventLoop):  # type: ignore
    class TestSelector(selectors.BaseSelector):
        def __init__(self, loop: 'SelectorTimeTrackingTestLoop', selector: selectors.BaseSelector) -> None:
            self._loop = loop
            self._selector = selector

        def __getattr__(self, item):
            return getattr(self._selector, item)

        # these are the magic and @abstractmethods, the others can be handled by __getattr__

        def register(self, *args, **kwargs):
            return self._selector.register(*args, **kwargs)

        def unregister(self, *args, **kwargs):
            return self._selector.unregister(*args, **kwargs)

        def select(self, timeout=None, *args, **kwargs):
            if timeout is not None:
                # instead of waiting for real seconds,
                # just deliver no events and let the event loop continue immediately.
                self._loop.advance_time(timeout)
                timeout = 0
            return self._selector.select(timeout, *args, **kwargs)

        def get_map(self, *args, **kwargs):
            return self._selector.get_map(*args, **kwargs)

        def __enter__(self):
            return self._selector.__enter__()

        def __exit__(self, *args):
            return self._selector.__exit__(*args)

    stuck_threshold = 100

    def __init__(self, selector: selectors.BaseSelector=None) -> None:
        super(SelectorTimeTrackingTestLoop, self).__init__(selector)
        self._selector = SelectorTimeTrackingTestLoop.TestSelector(self, self._selector)  # type: selectors.BaseSelector
        self._time = 0
        self.clear()

    def time(self):
        return self._time

    def advance_time(self, timeout):
        self._time += timeout
        self.steps.append(timeout)

    def clear(self) -> None:
        self.steps = []  # type: List[float]
        self.open_resources = 0
        self.resources = 0
        self.busy_count = 0

    @contextmanager
    def assert_cleanup(self) -> Generator['SelectorTimeTrackingTestLoop', None, None]:
        self.clear()
        yield self
        assert self.open_resources == 0
        self.clear()

    @contextmanager
    def assert_cleanup_steps(self, steps: List[float]) -> Generator['SelectorTimeTrackingTestLoop', None, None]:
        with self.assert_cleanup():
            yield self
            assert steps == self.steps


@pytest.fixture
def event_loop():
    loop = SelectorTimeTrackingTestLoop()
    loop.set_debug(True)
    asyncio.set_event_loop(loop)
    with loop.assert_cleanup():
        yield loop
    loop.close()


@pytest.fixture
def zmq_ctx():
    with zmq.Context() as ctx:
        yield ctx


@pytest.fixture
def zmq_aio_ctx():
    with zmq.asyncio.Context() as ctx:
        yield ctx


@pytest.fixture
def check_caplog(caplog):
    class CheckCaplog:
        level = logging.WARNING
        expected = set()

    check = CheckCaplog()
    try:
        yield check
    finally:
        records = [record for record in caplog.get_records('call')
                   if record.levelno >= check.level and record not in check.expected]
        if records:
            record_strs = "\n".join(f"    {record}" for record in records)
            pytest.fail(f"Unexpected log entries >= warning:\n{record_strs}")


async def assertTimeout(fut: asyncio.Future, timeout: float, shield: bool=False) -> Any:
    """
    Checks that the given coroutine or future is not fulfilled before a specified amount of time runs out.
    """
    if shield:
        fut = asyncio.shield(fut)
    try:
        result = await asyncio.wait_for(fut, timeout)
    except asyncio.TimeoutError:
        pass
    else:
        assert False, result


@contextmanager
def assertPassed(passed: float) -> Generator[None, None, None]:
    """
    A context manager that checks the code executed in its context has taken the exact given amount of time
    on the event loop.
    Naturally, exact timing can only work on a test event loop using simulated time.
    """
    begin = asyncio.get_event_loop().time()
    yield
    end = asyncio.get_event_loop().time()
    assert end - begin == passed


@contextmanager
def assertImmediate() -> Generator[None, None, None]:
    """
    Alias for assertPassed(0).
    """
    with assertPassed(0):
        yield

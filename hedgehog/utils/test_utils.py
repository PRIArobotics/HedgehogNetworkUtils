import asyncio.test_utils
import pytest
import zmq.asyncio
from contextlib import contextmanager


@pytest.fixture
def event_loop():
    class ZMQTimeTrackingTestLoop(zmq.asyncio.ZMQEventLoop, asyncio.test_utils.TestLoop):
        stuck_threshold = 100

        def __init__(self):
            super().__init__()
            self.clear()

        def _run_once(self):
            super(asyncio.test_utils.TestLoop, self)._run_once()
            # Update internals
            self.busy_count += 1
            self._timers = sorted(
                when for when in self._timers if when > self.time())
            # Time advance
            if self.time_to_go:
                when = self._timers.pop(0)
                step = when - self.time()
                self.steps.append(step)
                self.advance_time(step)
                self.busy_count = 0

        @property
        def stuck(self):
            return self.busy_count > self.stuck_threshold

        @property
        def time_to_go(self):
            return self._timers and (self.stuck or not self._ready)

        def clear(self):
            self.steps = []
            self.open_resources = 0
            self.resources = 0
            self.busy_count = 0

        @contextmanager
        def assert_cleanup(self):
            self.clear()
            yield self
            assert self.open_resources == 0
            self.clear()

        @contextmanager
        def assert_cleanup_steps(self, steps):
            with self.assert_cleanup():
                yield self
                assert steps == self.steps

    loop = ZMQTimeTrackingTestLoop()
    loop.set_debug(True)
    asyncio.set_event_loop(loop)
    with loop.assert_cleanup():
        yield loop
    loop.close()

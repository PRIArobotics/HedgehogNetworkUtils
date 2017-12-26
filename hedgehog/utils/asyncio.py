import asyncio
from aiostream import operator, stream

__all__ = ['repeat_func', 'repeat_func_eof', 'stream_from_queue', 'pipe', 'Actor', 'ActorException']

__DEFAULT = object()


@operator
def repeat_func(func, times=None, *, interval=0):
    """
    Repeats the result of a 0-ary function either indefinitely, or for a defined number of times.
    `times` and `interval` behave exactly like with `aiostream.create.repeat`.

    A useful idiom is to combine an indefinite `repeat_func` stream with `aiostream.select.takewhile`
    to terminate the stream at some point.
    """
    base = stream.repeat.raw((), times, interval=interval)
    return stream.starmap.raw(base, func)


@operator
def repeat_func_eof(func, eof, *, interval=0, use_is=False):
    """
    Repeats the result of a 0-ary function until an `eof` item is reached.
    The `eof` item itself is not part of the resulting stream; by setting `use_is` to true,
    an equality check is used for eof.
    `times` and `interval` behave exactly like with `aiostream.create.repeat`.
    """
    pred = (lambda item: item != eof) if not use_is else (lambda item: item is not eof)
    base = repeat_func.raw(func, interval=interval)
    return stream.takewhile.raw(base, pred)


def stream_from_queue(queue, eof=__DEFAULT, *, use_is=False):
    """
    Repeatedly gets an item from the given queue, until an item equal to `eof` (using `==` or `is`) is encountered.
    If no `eof` is given, the stream does not stop.
    """
    if eof is not __DEFAULT:
        return repeat_func_eof(queue.get, eof, use_is=use_is)
    else:
        return repeat_func(queue.get)


def pipe():
    """
    Returns a pair of objects that both support the operations `send` and `recv`
    that transmit arbitrary values between the two objects.
    """
    class _End(object):
        def __init__(self, r, w):
            self._r = r
            self._w = w

        async def send(self, msg):
            await self._w.put(msg)

        async def recv(self):
            return await self._r.get()

    a = asyncio.Queue()
    b = asyncio.Queue()
    return _End(a, b), _End(b, a)


class Active(object):
    """
    An Active object is one that can be "started" and "stopped", meaning it will then execute code asynchronously.
    Starting and stopping can be done manually, or by using the object as a context manager, with `async with`.
    """

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self) -> None:
        pass  # pragma: no cover

    async def stop(self) -> None:
        pass  # pragma: no cover


class ActorException(Exception):
    pass


class Actor(Active):
    """
    An `Actor` encapsulates a task that is executed on an event loop, and two pipes for communicating with that task.

    The command pipe is used for communication initiated by the actor's creator, i.e. commands and their results.
    The event pipe is meant for communication initiated by the actor's task.

    An actor is an asynchronous content manager and can be used with `async with`.
    When entering the block or calling `start()`, the actor's `run` method is executed as a task,
    and when exiting or calling `stop()`, the actor is asked to shut down.
    Calling `stop(block=True)` asks the actor for shutdown without waiting for the shutdown to complete.
    This is useful for actors that produce events during shutdown.

    `run` has to conform to a simple contract:
    - As the first message on the event pipe, the binary string `b'$START'` must be sent.
      This indicates finished actor initialization, and only then will the caller enter the actor's context.
    - After that, the actor must not send the binary string `b'$TERM'` as an event.
    - When the binary command `b'$TERM'` is received, the run method must eventually terminate.
      It may send additional events to the caller during that time.

    The binary string `b'$TERM'` will automatically be sent as an event to the caller when `run` exits.
    That means the actor's caller should watch for this to know when the actor task terminates,
    be it by a shutdown request, an error, or regularly.
    """

    class Task(object):
        def __init__(self, run):
            self.cmd_pipe, self._cmd_pipe = pipe()
            self.evt_pipe, self._evt_pipe = pipe()

            self._run = run
            self._future = None
            self._state = None

        async def start(self):
            assert self._state is None

            async def _run():
                try:
                    await self._run(self._cmd_pipe, self._evt_pipe)
                finally:
                    await self._evt_pipe.send(b'$TERM')

            self._future = asyncio.ensure_future(_run())
            self._state = 'running'

            start = await self.evt_pipe.recv()
            if start == b'$TERM':
                # run terminated during initialization.
                # Initialization may have raised an exception, in that case reraise it:
                await self._future
                # otherwise, run did not send any events, including b'$START'; fall-through
            if start != b'$START':
                raise ActorException("run() must send b'$START' to signal actor initialization!")

        async def destroy(self, block=True):
            assert self._state is not None

            if self._future.done():
                self._state = 'terminated'

            if self._state == 'running':
                await self.cmd_pipe.send(b'$TERM')
                self._state = 'destroyed'

            if block and self._state == 'destroyed':
                while await self.evt_pipe.recv() != b'$TERM':
                    pass
                self._state = 'terminated'

            if block and self._state == 'terminated':
                await self._future

    async def start(self):
        self._task = Actor.Task(self.run)
        try:
            await self._task.start()
        except Exception:
            self._task = None
            raise
        return self

    async def stop(self, block=True):
        try:
            await self._task.destroy(block)
        finally:
            if block:
                self._task = None

    @property
    def cmd_pipe(self):
        return self._task.cmd_pipe

    @property
    def evt_pipe(self):
        return self._task.evt_pipe

    async def run(self, cmd_pipe, evt_pipe):
        raise NotImplementedError()  # pragma: no cover

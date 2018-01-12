from typing import cast, Any, AsyncIterator, Awaitable, Callable, Tuple, TypeVar, Union

import asyncio
from aiostream import operator, stream

__all__ = ['repeat_func', 'repeat_func_eof', 'stream_from_queue', 'pipe', 'Actor', 'ActorException']

__DEFAULT = object()


T = TypeVar('T')


@operator
def repeat_func(func: Callable[[], Union[T, Awaitable[T]]], times: int=None, *, interval: float=0) -> AsyncIterator[T]:
    """
    Repeats the result of a 0-ary function either indefinitely, or for a defined number of times.
    `times` and `interval` behave exactly like with `aiostream.create.repeat`.

    A useful idiom is to combine an indefinite `repeat_func` stream with `aiostream.select.takewhile`
    to terminate the stream at some point.
    """
    base = stream.repeat.raw((), times, interval=interval)
    return cast(AsyncIterator[T], stream.starmap.raw(base, func))


@operator
def repeat_func_eof(func: Callable[[], Union[T, Awaitable[T]]], eof: Any, *, interval: float=0, use_is: bool=False) -> AsyncIterator[T]:
    """
    Repeats the result of a 0-ary function until an `eof` item is reached.
    The `eof` item itself is not part of the resulting stream; by setting `use_is` to true,
    an equality check is used for eof.
    `times` and `interval` behave exactly like with `aiostream.create.repeat`.
    """
    pred = (lambda item: item != eof) if not use_is else (lambda item: (item is not eof))
    base = repeat_func(func, interval=interval)
    return cast(AsyncIterator[T], stream.takewhile.raw(base, pred))


def stream_from_queue(queue: asyncio.Queue, eof: Any=__DEFAULT, *, use_is: bool=False) -> AsyncIterator[Any]:
    """
    Repeatedly gets an item from the given queue, until an item equal to `eof` (using `==` or `is`) is encountered.
    If no `eof` is given, the stream does not stop.
    """
    if eof is not __DEFAULT:
        return cast(AsyncIterator[Any], repeat_func_eof(queue.get, eof, use_is=use_is))
    else:
        return cast(AsyncIterator[Any], repeat_func(queue.get))


class PipeEnd(object):
    def __init__(self, r: asyncio.Queue, w: asyncio.Queue) -> None:
        self._r = r
        self._w = w

    async def send(self, msg: Any) -> None:
        await self._w.put(msg)

    async def recv(self)-> Any:
        return await self._r.get()


def pipe() -> Tuple[PipeEnd, PipeEnd]:
    """
    Returns a pair of objects that both support the operations `send` and `recv`
    that transmit arbitrary values between the two objects.
    """

    a = asyncio.Queue()  # type: asyncio.Queue
    b = asyncio.Queue()  # type: asyncio.Queue
    return PipeEnd(a, b), PipeEnd(b, a)


class Active(object):
    """
    An Active object is one that can be "started" and "stopped", meaning it will then execute code asynchronously.
    Starting and stopping can be done manually, or by using the object as a context manager, with `async with`.

    An Active object can be awaited to wait for it finishing, either because it was stopped somewhere
    or because it terminated on its own.
    To run an active object until it has stopped, the following idiom can be used:

        async with active:
            # do anything while `active` is running
            await active
            # do anything after `active` is done, but before it was cleaned up

    Obviously, unless the active object terminates or is stopped somewhere,
    this code will not advance past the `await` line.
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

    def __await__(self) -> Any:
        raise NotImplementedError  # pragma: nocover


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
    Calling `stop(block=False)` asks the actor for shutdown without waiting for the shutdown to complete.
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
        def __init__(self, run: Callable[[PipeEnd, PipeEnd], Awaitable[None]]) -> None:
            self.cmd_pipe, self._cmd_pipe = pipe()
            self.evt_pipe, self._evt_pipe = pipe()

            self._run = run
            self._future = None  # type: asyncio.Future[None]
            self._state = None  # type: str

        async def start(self) -> None:
            assert self._state is None

            async def _run() -> None:
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

        async def destroy(self, block: bool=True) -> None:
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

        def __await__(self):
            yield from self._future.__await__()

    def __init__(self) -> None:
        super(Actor, self).__init__()
        self._task = None  # type: Actor.Task

    async def start(self) -> None:
        self._task = Actor.Task(self.run)
        try:
            await self._task.start()
        except Exception:
            self._task = None
            raise

    async def stop(self, block: bool=True)-> None:
        try:
            await self._task.destroy(block)
        finally:
            if block:
                self._task = None

    def __await__(self):
        yield from self._task.__await__()

    @property
    def cmd_pipe(self) -> PipeEnd:
        return self._task.cmd_pipe

    @property
    def evt_pipe(self) -> PipeEnd:
        return self._task.evt_pipe

    async def run(self, cmd_pipe: PipeEnd, evt_pipe: PipeEnd) -> None:
        raise NotImplementedError()  # pragma: no cover

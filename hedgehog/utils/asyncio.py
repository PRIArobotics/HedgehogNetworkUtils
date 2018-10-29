from typing import cast, Any, AsyncIterator, Awaitable, Callable, TypeVar, Union

import asyncio
from aiostream import operator, stream

__all__ = ['repeat_func', 'repeat_func_eof', 'stream_from_queue']

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
    return cast(AsyncIterator[T], stream.starmap.raw(base, func, task_limit=1))


@operator
def repeat_func_eof(func: Callable[[], Union[T, Awaitable[T]]], eof: Any, *, interval: float=0, use_is: bool=False) -> AsyncIterator[T]:
    """
    Repeats the result of a 0-ary function until an `eof` item is reached.
    The `eof` item itself is not part of the resulting stream; by setting `use_is` to true,
    eof is checked by identity rather than equality.
    `times` and `interval` behave exactly like with `aiostream.create.repeat`.
    """
    pred = (lambda item: item != eof) if not use_is else (lambda item: (item is not eof))
    base = repeat_func.raw(func, interval=interval)
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

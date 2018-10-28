from typing import Callable, Dict, TypeVar
from itertools import zip_longest

__all__ = ['expect', 'expect_all', 'coroutine', 'SimpleDecorator', 'Registry']


def expect(a, b):
    """\
    Asserts that two values are the same.
    """
    assert a == b


def expect_all(a, b):
    """\
    Asserts that two iterables contain the same values.
    """
    assert all(_a == _b for _a, _b in zip_longest(a, b))


def coroutine(func):
    """
    A decorator to wrap a generator function into a callable interface.

        >>> @coroutine
        ... def sum(count):
        ...     sum = 0
        ...     for _ in range(0, count):
        ...         # note that generator arguments are passed as a tuple, hence `num, = ...` instead of `num = ...`
        ...         num, = yield sum
        ...         sum += num
        ...     yield sum
        ...
        >>> add = sum(2)
        >>> add(2)
        2
        >>> add(3)
        5
        >>> add(4)
        Traceback (most recent call last):
          ...
        StopIteration

    As you can see, this lets you keep state between calls easily, as expected from a generator, while calling the
    function looks like a function. The same without `@coroutine` would look like this:

        >>> def sum(count):
        ...     sum = 0
        ...     for _ in range(0, count):
        ...         num = yield sum
        ...         sum += num
        ...     yield sum
        ...
        >>> add = sum(2)
        >>> next(add)  # initial next call is necessary
        0
        >>> add.send(2)  # to call the function, next or send must be used
        2
        >>> add.send(3)
        5
        >>> add.send(4)
        Traceback (most recent call last):
          ...
        StopIteration

    Here is an example that shows how to translate traditional functions to use this decorator:

        >>> def foo(a, b):
        ...     # do some foo
        ...     return a + b
        ...
        >>> def bar(c):
        ...     # do some bar
        ...     return 2*c
        ...
        >>> foo(1, 2)
        3
        >>> bar(3)
        6

        >>> @coroutine
        ... def func_maker():
        ...     a, b = yield
        ...     # do some foo
        ...     c, = yield foo(a, b)
        ...     # do some bar
        ...     yield bar(c)
        ...
        >>> func_once = func_maker()
        >>> func_once(1, 2)
        3
        >>> func_once(3)
        6

    The two differences are that a) using traditional functions, func1 and func2 don't share any context and b) using
    the decorator, both calls use the same function name, and calling the function is limited to wice (in this case).
    """

    def decorator(*args, **kwargs):
        generator = func(*args, **kwargs)
        next(generator)
        return lambda *args: generator.send(args)
    return decorator


T = TypeVar('T')
SimpleDecorator = Callable[[T], T]

KT = TypeVar('KT')
VT = TypeVar('VT')


class Registry(Dict[KT, VT]):
    def register(self, key: KT) -> SimpleDecorator[VT]:
        def decorator(value: VT) -> VT:
            self[key] = value
            return value
        return decorator

def coroutine(func):
    """
    A decorator to wrap a generator function into a callable interface.

        @coroutine
        def sum(count):
            sum = 0
            for _ in range(0, count):
                # note that generator arguments are passed as a tuple, hence `num, = ...` instead of `num = ...`
                num, = yield sum
                sum += num
            yield sum

        add = sum(2)
        add(2) # returns 2
        add(3) # returns 5
        add(4) # raises StopIteration: only two numbers can be added

    As you can see, this lets you keep state between calls easily, as expected from a generator, while calling the
    function looks like a function. The same without `@coroutine` would look like this:

        def sum(count):
            sum = 0
            for _ in range(0, count):
                num = yield sum
                sum += num
            yield sum

        add = sum(2)
        # initial next call is necessary
        next(add)
        # to call the function, next or send must be used
        add.send(2) # returns 2
        add.send(3) # returns 5
        add.send(4) # raises StopIteration: only two numbers can be added

    Here is an example that shows how to translate traditional functions to use this decorator:

        def func1(a, b):
            # do some foo
            return foo()

        def func2(c):
            # do some bar
            return bar()

        result1 = func1(a, b)
        result2 = func2(c)

        @coroutine
        def func_maker():
            a, b = yield
            # do some foo
            c, = yield foo()
            # do some bar
            yield bar()

        func_once = func_maker()
        result1 = func_once(a, b)
        result2 = func_once(c)

    The two differences are that a) using traditional functions, func1 and func2 don't share any context and b) using
    the decorator, both calls use the same function name, and calling the function is limited to wice (in this case).
    """

    def decorator(*args, **kwargs):
        generator = func(*args, **kwargs)
        next(generator)
        return lambda *args: generator.send(args)
    return decorator

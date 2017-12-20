import pytest
from aiostream import stream
from contextlib import contextmanager

from hedgehog.utils.asyncio import pipe, Actor, ActorException


@contextmanager
def assert_actor_cleanup(actor):
    try:
        yield actor
    finally:
        with pytest.raises(AttributeError):
            actor.cmd_pipe


@pytest.mark.asyncio
async def test_pipe():
    a, b = pipe()

    await a.send("foo")
    assert await b.recv() == "foo"


@pytest.mark.asyncio
async def test_actor():
    class MyActor(Actor):
        def __init__(self, greeting):
            self.greeting = greeting

        async def greet(self, msg):
            await self.cmd_pipe.send((b'GREET', msg))
            return await self.cmd_pipe.recv()

        async def agreet(self, msg):
            await self.cmd_pipe.send((b'AGREET', msg))

        async def run(self, cmd_pipe, evt_pipe):
            await evt_pipe.send(b'$START')

            expected = [(b'GREET', "world"), (b'AGREET', "world"), b'$TERM']

            async with stream.count().stream() as streamer:
                i = -1
                async for i in streamer:
                    cmd = await cmd_pipe.recv()
                    assert cmd == expected[i]

                    if cmd == b'$TERM':
                        break

                    cmd, *payload = cmd

                    if cmd == b'GREET':
                        msg, = payload
                        await cmd_pipe.send(f"{self.greeting} {msg}")
                    elif cmd == b'AGREET':
                        msg, = payload
                        await evt_pipe.send((b'AGREET', f"{self.greeting} {msg}"))
                assert i == len(expected) - 1
            # this happens when terminating the actor, so is always ignored
            await evt_pipe.send((b'IGNORED',))

    with assert_actor_cleanup(MyActor("hello")) as a:
        async with a:
            assert await a.greet("world") == "hello world"
            await a.agreet("world")
            assert await a.evt_pipe.recv() == (b'AGREET', "hello world")


@pytest.mark.asyncio
async def test_faulty_actor():
    class FaultyActor(Actor):
        async def run(self, cmd_pipe, evt_pipe):
            pass

    with pytest.raises(ActorException):
        with assert_actor_cleanup(FaultyActor()) as a:
            async with a:
                pass


@pytest.mark.asyncio
async def test_failing_actor():
    class MyException(Exception):
        pass

    class FailingActor(Actor):
        async def run(self, cmd_pipe, evt_pipe):
            await evt_pipe.send(b'$START')
            raise MyException()

    with pytest.raises(MyException):
        with assert_actor_cleanup(FailingActor()) as a:
            async with a:
                pass


@pytest.mark.asyncio
async def test_init_failing_actor():
    class MyException(Exception):
        pass

    class InitFailingActor(Actor):
        async def run(self, cmd_pipe, evt_pipe):
            raise MyException()

    with pytest.raises(MyException):
        with assert_actor_cleanup(InitFailingActor()) as a:
            async with a:
                pass

import pytest
from hedgehog.utils.test_utils import event_loop

import asyncio
import time
import zmq.asyncio

from hedgehog.utils.zmq.pipe import pipe, extended_pipe
from hedgehog.utils.zmq.actor import Actor, CommandRegistry
from hedgehog.utils.zmq.async_socket import Socket
from hedgehog.utils.zmq.timer import Timer


# Pytest fixtures
event_loop


class TestPipe(object):
    def test_pipe(self):
        ctx = zmq.Context()

        a, b = pipe(ctx, endpoint='inproc://endpoint')

        a.signal()
        b.wait()

    def test_extended_pipe(self):
        ctx = zmq.Context()

        a, b = extended_pipe(ctx, endpoint='inproc://endpoint')

        obj = object()
        a.push(obj)
        a.signal()
        b.wait()
        assert b.pop() is obj


class TestAsyncSocket(object):
    @pytest.mark.asyncio
    async def test_async_socket(self, event_loop):
        with event_loop.assert_cleanup_steps(steps=[1]):
            ctx = zmq.asyncio.Context()

            a, b = (Socket(ctx, zmq.PAIR).configure(hwm=1000, linger=0) for _ in range(2))
            a.bind('inproc://endpoint')
            b.connect('inproc://endpoint')

            task = asyncio.ensure_future(b.wait())
            await asyncio.sleep(1)
            await a.signal()
            await task


class TestActor(object):
    def test_actor_termination(self):
        def task(ctx, cmd_pipe, evt_pipe):
            evt_pipe.signal()

            cmd_pipe.recv_expect(b'do')

        ctx = zmq.Context()
        actor = Actor(ctx, task)

        actor.cmd_pipe.send(b'do')

        # await the actor terminating
        actor.evt_pipe.recv_expect(b'$TERM')

    def test_actor_destruction(self):
        def task(ctx, cmd_pipe, evt_pipe):
            evt_pipe.signal()
            cmd_pipe.recv_expect(b'do')
            cmd_pipe.recv_expect(b'$TERM')

        ctx = zmq.Context()
        actor = Actor(ctx, task)

        actor.cmd_pipe.send(b'do')

        # this triggers and awaits actor termination
        actor.destroy()

    def test_actor_destruction_event(self):
        def task(ctx, cmd_pipe, evt_pipe):
            evt_pipe.signal()
            cmd_pipe.recv_expect(b'$TERM')
            evt_pipe.send(b'event')
            evt_pipe.recv_expect(b'reply')

        ctx = zmq.Context()
        actor = Actor(ctx, task)

        # this triggers actor termination
        actor.destroy(block=False)
        actor.evt_pipe.recv_expect(b'event')
        actor.evt_pipe.send(b'reply')
        actor.evt_pipe.recv_expect(b'$TERM')

    def test_command_registry(self):
        registry = CommandRegistry()

        def handler(payload):
            assert payload == b'payload'

        registry.register(b'test', handler)

        registry.handle((b'test', b'payload'))

    def test_command_registry_decorator(self):
        registry = CommandRegistry()

        @registry.command(b'test')
        def handler(payload):
            assert payload == b'payload'

        registry.handle((b'test', b'payload'))


class TestTimer(object):
    def test_order(self):
        ctx = zmq.Context()
        with Timer(ctx) as timer:
            a = timer.register(0.010, "a")
            time.sleep(0.015)  # 0: a, 10: a; time=15
            b = timer.register(0.010, "b")
            time.sleep(0.010)  # 15: b, 20: a, 25: b; time=25
            timer.unregister(a)
            c = timer.register(0.005, "c", repeat=False)
            time.sleep(0.015)  # 30:c, 35:b; time=40
            timer.unregister(b)
            time.sleep(0.010)  # time=50

            timers = [a, a, b, a, b, c, b]

            events = timer.evt_pipe.poll(0)
            while events & zmq.POLLIN:
                timer.evt_pipe.recv_expect(b'TIMER')
                then, t = timer.evt_pipe.pop()
                assert t is timers.pop(0)
                events = timer.evt_pipe.poll(0)
            assert len(timers) == 0

    @pytest.mark.skip
    def test_load(self):
        ctx = zmq.Context()
        with Timer(ctx) as timer:
            ts = [timer.register(0.01, id) for id in range(100)]
            time.sleep(0.015)
            for t in ts:
                timer.unregister(t)

            timers = [0 for t in ts]

            events = timer.evt_pipe.poll(0)
            while events & zmq.POLLIN:
                timer.evt_pipe.recv_expect(b'TIMER')
                then, t = timer.evt_pipe.pop()
                timers[t.aux] += 1
                events = timer.evt_pipe.poll(0)
            assert timers == [2 for t in ts]

    def test_terminate(self):
        ctx = zmq.Context()
        with Timer(ctx) as timer:
            timer.register(0.1)

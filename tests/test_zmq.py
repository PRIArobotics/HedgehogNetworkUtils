import unittest
import zmq
from hedgehog.utils.zmq.pipe import pipe, extended_pipe
from hedgehog.utils.zmq.actor import Actor, CommandRegistry


class PipeTests(unittest.TestCase):
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
        self.assertTrue(b.pop() is obj)


class ActorTests(unittest.TestCase):
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

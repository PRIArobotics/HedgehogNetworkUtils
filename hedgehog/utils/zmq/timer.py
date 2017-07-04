from typing import Any, List, Tuple

import bisect
import time
import zmq
from collections import namedtuple

from . import Active
from .actor import Actor, CommandRegistry
from .poller import Poller


TimerDefinition = namedtuple('TimerDefinition', ('interval', 'aux', 'repeat'))


class TimerActor(object):
    def __init__(self, ctx: zmq.Context, cmd_pipe, evt_pipe) -> None:
        self.ctx = ctx
        self.cmd_pipe = cmd_pipe
        self.evt_pipe = evt_pipe

        self.timers = []  # type: List[Tuple[float, TimerDefinition]]
        self.poller = Poller()
        self.register_cmd_pipe()

        self.run()

    def register_cmd_pipe(self) -> None:
        registry = CommandRegistry()
        self.poller.register(self.cmd_pipe, zmq.POLLIN, lambda: registry.handle(self.cmd_pipe.recv_multipart()))

        @registry.command(b'REG')
        def handle_reg():
            timer = self.cmd_pipe.pop()  # type: TimerDefinition
            then = time.time() if timer.repeat else time.time() + timer.interval
            bisect.insort(self.timers, (then, timer))

        @registry.command(b'UNREG')
        def handle_unreg():
            timer = self.cmd_pipe.pop()  # type: TimerDefinition
            index = next((i for i, (_, x) in enumerate(self.timers) if x is timer), None)
            if index is not None:
                del self.timers[index]

        @registry.command(b'$TERM')
        def handle_term():
            for socket in list(self.poller.sockets):
                self.poller.unregister(socket)

    def run(self) -> None:
        # Signal actor successfully initialized
        self.evt_pipe.signal()

        while len(self.poller.sockets) > 0:
            now = time.time()
            while len(self.timers) > 0 and self.timers[0][0] < now:
                then, timer = self.timers.pop(0)
                self.evt_pipe.push((now, timer))
                self.evt_pipe.send(b'TIMER')
                if timer.repeat:
                    bisect.insort(self.timers, (time.time() + timer.interval, timer))

            if len(self.timers) == 0:
                events = self.poller.poll()
            else:
                then, timer = self.timers[0]
                events = self.poller.poll((then - now)*1000)

            for _, _, handler in events:
                handler()


class Timer(Active):
    def __init__(self, ctx: zmq.Context) -> None:
        self.ctx = ctx
        self.actor = None  # type: TimerActor

    @property
    def cmd_pipe(self):
        return self.actor.cmd_pipe

    @property
    def evt_pipe(self):
        return self.actor.evt_pipe

    def register(self, interval: float, aux: Any=None, repeat=True) -> TimerDefinition:
        timer = TimerDefinition(interval, aux, repeat)
        self.cmd_pipe.push(timer)
        self.cmd_pipe.send(b'REG')
        return timer

    def unregister(self, timer: TimerDefinition) -> None:
        self.cmd_pipe.push(timer)
        self.cmd_pipe.send(b'UNREG')

    def start(self):
        self.actor = Actor(self.ctx, TimerActor)

    def stop(self):
        if self.actor is not None:
            self.actor.destroy()
            self.actor = None

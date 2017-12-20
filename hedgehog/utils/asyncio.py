import asyncio

__all__ = ['pipe', 'Actor', 'ActorException']


def pipe():
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


class ActorException(Exception):
    pass


class Actor(object):
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

            if self._state == 'running':
                await self.cmd_pipe.send(b'$TERM')
                self._state = 'destroyed'

            if block and self._state == 'destroyed':
                while (await self.evt_pipe.recv()) != b'$TERM':
                    pass
                self._state = 'terminated'
                await self._future

    async def __aenter__(self):
        self._task = Actor.Task(self.run)
        try:
            await self._task.start()
        except Exception:
            self._task = None
            raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._task.destroy()
        finally:
            self._task = None

    @property
    def cmd_pipe(self):
        return self._task.cmd_pipe

    @property
    def evt_pipe(self):
        return self._task.evt_pipe

    async def run(self, cmd_pipe, evt_pipe):
        raise NotImplementedError()  # pragma: no cover

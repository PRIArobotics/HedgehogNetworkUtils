import asyncio


def pipe():
    class _End(object):
        def __init__(self, r, w):
            self.r = r
            self.w = w

        async def send(self, msg):
            await self.w.put(msg)

        async def recv(self):
            return await self.r.get()

    a = asyncio.Queue()
    b = asyncio.Queue()
    return _End(a, b), _End(b, a)


class Actor(object):
    class Task(object):
        def __init__(self, run, *args, **kwargs):
            self.cmd_pipe, cmd_pipe = pipe()
            self.evt_pipe, evt_pipe = pipe()

            async def _run():
                await run(cmd_pipe, evt_pipe, *args, **kwargs)
                await evt_pipe.send(b'$TERM')

            self._future = asyncio.ensure_future(_run())
            self._state = 'running'

        async def destroy(self, block=True):
            if self._state == 'running':
                await self.cmd_pipe.send(b'$TERM')
                self._state = 'destroyed'

            if block and self._state == 'destroyed':
                while (await self.evt_pipe.recv()) != b'$TERM':
                    pass
                self._state = 'terminated'

    async def __aenter__(self):
        self._task = Actor.Task(self.run)
        start = await self._task.evt_pipe.recv()
        if start != b'$START':
            raise ValueError("run() must send b'$START' to signal actor initialization!")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._task.destroy()

    @property
    def cmd_pipe(self):
        return self._task.cmd_pipe

    @property
    def evt_pipe(self):
        return self._task.evt_pipe

    async def run(self, cmd_pipe, evt_pipe):
        raise NotImplementedError()

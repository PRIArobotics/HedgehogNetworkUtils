import pytest
import asyncio


class TestAsyncActor(object):
    @pytest.mark.asyncio
    async def test_async_actor(self):
        import hedgehog.utils.asyncio

        class MyActor(hedgehog.utils.asyncio.Actor):
            async def command(self, cmd):
                await t.cmd_pipe.send((b'CMD', cmd))

            async def run(self, cmd_pipe, evt_pipe):
                await evt_pipe.send(b'$START')

                # while True:
                #     cmd = await cmd_pipe.recv()
                #     if cmd == b'$TERM':
                #         break
                #
                #     cmd, *payload = cmd
                #     if cmd == b'CMD':
                #         print(*payload)

                cmd = await cmd_pipe.recv()
                assert cmd == (b'CMD', "hello")

                cmd = await cmd_pipe.recv()
                assert cmd == b'$TERM'

        try:
            async with MyActor() as t:
                await t.command("hello")
        except asyncio.CancelledError:
            import traceback
            traceback.print_exc()

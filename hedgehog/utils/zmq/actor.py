import logging
import threading

from .pipe import extended_pipe

logger = logging.getLogger(__name__)


class Actor(object):
    """
    An Actor represents a handle to another thread, providing two pipes for commands and events, respectively.
    The Actor needs a handler function with the signature `handler(ctx, cmd_pipe, evt_pipe, *args, **kwargs)`, which
    will be run in a new thread. The original thread will be called the "caller" here, and the new thread the "actor".

    The command pipe is used to send command messages to the actor and receive replies from it, while the event pipe is
    used to receive event messages from the actor and send replies back. Generally speaking, the actor will only poll
    the command socket and only initiate communication on the event socket, and the caller will do the exact opposite.

    The actor will be started when constructing an Actor object. It must signal the event socket when initialization is
    completed, only then will the Actor constructor return. Upon completion, `b'$TERM'` (termination event) will be sent
    to the caller. The `destroy()` method will send `b'$TERM'` (destroy command) to the actor and wait for the thread's
    completion, i.e. the termination event.

    Handling the destroy command is the actor's handler function's responsibility; not supporting this command must be
    documented. Note that waiting for event responses can lead to a deadlock if `destroy()` is used, as `destroy()` will
    discard any further events except for the termination event. If you expect replies on the event channel, either
    handle further commands while waiting for replies, or advise your clients to use `destroy(block=False)` and handle
    events after issuing destruction.
    """

    def __init__(self, ctx, actor, *args, **kwargs):
        self.cmd_pipe, self._cmd_pipe = extended_pipe(ctx)
        self.evt_pipe, self._evt_pipe = extended_pipe(ctx)

        self._actor = actor
        self._args = (ctx, self._cmd_pipe, self._evt_pipe) + args
        self._kwargs = kwargs

        self.thread = threading.Thread(target=self.run)
        # we manage threads exiting ourselves!
        self.thread.daemon = False
        self.thread.start()

        # Mandatory handshake for new actor so that constructor returns only
        # when actor has also initialized. This eliminates timing issues at
        # application start up.
        self.evt_pipe.wait()

    def run(self):
        self._actor(*self._args, **self._kwargs)
        self._cmd_pipe.close()

        self._evt_pipe.configure(sndtimeo=0)
        self._evt_pipe.send(b'$TERM')
        self._evt_pipe.close()

    def destroy(self, block=True):
        """
        Ask the actor thread to terminate. If `block=False` is given, the caller thread can receive further events,
        including the termination event when the actor was really destroyed. When using that option, the event pipe
        should be manually closed after receiving the termination event. In any case, no more commands can be sent after
        calling `destroy()`.

        :param block: Whether to block until the actor has terminated, defaults to `True`
        """
        self.cmd_pipe.configure(sndtimeo=0)
        self.cmd_pipe.send_unicode('$TERM')
        self.cmd_pipe.close()

        # the option to not block is useful to receive events that happened after the destroy command
        if block:
            # The client thread can't handle the events, so ignoring events is a fair course of action.
            while self.evt_pipe.recv_multipart() != [b'$TERM']:
                pass
            self.evt_pipe.close()


class CommandRegistry(object):
    def __init__(self):
        self.cmds = {}

    def register(self, command, callback):
        self.cmds[command] = callback

    def command(self, command):
        """
        reg.command(a)(b) is the same as reg.register(a, b).
        This function is meant to be used as a decorator:

        @reg.command(a)
        def b():
            pass
        """
        return lambda callback: self.register(command, callback)

    def handle(self, msg):
        """
        Splits the message into command and payload and dispatches it to a handler.
        The first part of the received multipart message is the command, which is mapped to a handler. The rest of the
        message is passed to the handler, each part being an individual argument.
        """
        command, *payload = msg
        self.cmds[command](*payload)

import zmq
from pyre import zhelper


def pipe(ctx=None, hwm=1000):
    if ctx is None:
        ctx = zmq.Context.instance()
    return zhelper.zcreate_pipe(ctx, hwm)
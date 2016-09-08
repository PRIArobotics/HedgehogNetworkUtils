import zmq

from itertools import zip_longest


class Socket(zmq.Socket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def configure(self, hwm=None, rcvtimeo=None, sndtimeo=None, linger=None):
        if hwm is not None:
            self.set_hwm(hwm)
        if rcvtimeo is not None:
            self.setsockopt(zmq.RCVTIMEO, rcvtimeo)
        if sndtimeo is not None:
            self.setsockopt(zmq.SNDTIMEO, sndtimeo)
        if linger is not None:
            self.setsockopt(zmq.LINGER, linger)
        return self

    def signal(self):
        self.send(b'')

    def wait(self):
        self.recv_expect(b'')

    def recv_expect(self, data=b''):
        recvd = self.recv()
        assert recvd == data

    def recv_multipart_expect(self, data=(b'',)):
        recvd = self.recv_multipart()
        assert all((a == b for a, b in zip_longest(recvd, data)))


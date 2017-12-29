class Active(object):
    """
    An Active object is one that can be "started" and "stopped", meaning it will then execute code on its own (e.g. in a
    background thread). Starting and stopping can be done manually, or by using the object as a context manager, with
    `with`.
    """

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self) -> None:
        pass  # pragma: no cover

    def stop(self) -> None:
        pass  # pragma: no cover

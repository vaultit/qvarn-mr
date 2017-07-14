class HandlerVersionError(Exception):
    """Raised by reduce processor when some resources have different version than handler version."""

    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key


class BusyListenerError(Exception):
    pass

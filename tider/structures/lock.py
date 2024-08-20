class DummyLock:
    """Pretending to be a lock."""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


class RedisLock:
    pass

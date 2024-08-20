from tider.network import Request


class BaseDupeFilter:
    @classmethod
    def from_settings(cls, settings):
        return cls()

    def request_seen(self, request: Request) -> bool:
        return False

    def open(self):
        pass

    def close(self, reason: str):
        pass

    def log(self, request):
        pass

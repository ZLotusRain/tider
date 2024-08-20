"""Base file store class"""


class FilesStore:
    def persist_file(self, **kwargs):
        raise NotImplementedError("FilesStore object must implement :meth: `persist_file`")

    def stat_file(self, **kwargs):
        return {}

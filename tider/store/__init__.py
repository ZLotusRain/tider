"""Base file store class"""
from typing import Optional

from tider.store.filesystem import FSFilesStore
from tider.store.ftp import FTPFilesStore
from tider.store.alioss import AliOSSFilesStore

from tider.utils.misc import create_instance

__all__ = ('get_fsm', 'create_fsm')


_filestore_manager = None


class FilesStoreManager:

    STORE_SCHEMAS = {
        "": FSFilesStore,
        "file": FSFilesStore,
        "ftp": FTPFilesStore,
        "alioss": AliOSSFilesStore
    }

    def __init__(self, tider, store_schemas=None):
        self.tider = tider
        store_schemas = store_schemas or {}
        for k, v in store_schemas.items():
            self.mount(k, v)
        self._store_schemas_instances = {}

    def _instantiate_schema(self, schema):
        store_cls = self.STORE_SCHEMAS[schema]
        self._store_schemas_instances[schema] = create_instance(
            objcls=store_cls,
            tider=self.tider,
            settings=self.tider.settings
        )

    def get_fs(self, schema):
        if schema not in self._store_schemas_instances:
            self._instantiate_schema(schema)
        return self._store_schemas_instances[schema]

    @classmethod
    def from_tider(cls, tider):
        settings = tider.settings

        ftp_store = cls.STORE_SCHEMAS["ftp"]
        ftp_store.FTP_USERNAME = settings["FTP_USER"]
        ftp_store.FTP_PASSWORD = settings["FTP_PASSWORD"]
        ftp_store.USE_ACTIVE_MODE = settings.getbool("FTP_USE_ACTIVE_MODE")

        store_schemas = settings["STORE_SCHEMAS"]
        return cls(tider, store_schemas=store_schemas)

    def mount(self, schema, fs_cls):
        if not hasattr(fs_cls, 'persist_file'):
            raise ValueError("FileStore class must implement the method `persist_file()`")
        self.STORE_SCHEMAS[schema] = fs_cls

    def persist_file(self, schema, path, buf, **kwargs):
        return self.get_fs(schema).persist_file(path=path, buf=buf, **kwargs)

    def stat_file(self, schema, path, **kwargs):
        return self.get_fs(schema).stat_file(path=path, **kwargs)

    def close(self):
        for schema in self._store_schemas_instances:
            self._store_schemas_instances[schema].close()


def create_fsm(tider):
    global _filestore_manager
    if _filestore_manager is None:
        _filestore_manager = FilesStoreManager.from_tider(tider)


def get_fsm() -> Optional[FilesStoreManager]:
    return _filestore_manager


def close_fsm():
    _filestore_manager and _filestore_manager.close()

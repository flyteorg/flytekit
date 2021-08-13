import fsspec
from fsspec.core import split_protocol
from fsspec.registry import known_implementations

from flytekit.extend import DataPersistence, DataPersistencePlugins


class FSSpecPersistence(DataPersistence):

    def __init__(self):
        super(FSSpecPersistence, self).__init__(name="fsspec-persistence", default_prefix=None)

    @staticmethod
    def _get_filesystem(path: str) -> fsspec.AbstractFileSystem:
        protocol = split_protocol(path)
        return fsspec.filesystem(protocol)

    def exists(self, path: str) -> bool:
        fs = self._get_filesystem(path)
        return fs.exists(path)

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self._get_filesystem(from_path)
        return fs.get(from_path, to_path, recursive=recursive)

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self._get_filesystem(to_path)
        return fs.put(from_path, to_path, recursive=recursive)

    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths) -> str:
        paths = list(paths)  # make type check happy
        if add_prefix:
            paths = paths.insert(0, self.default_prefix)
        path = f"{'/'.join(paths)}"
        if add_protocol:
            raise AssertionError("Fsspec supports multiple protocols, so cannot add one protocol!")
        return path


def _register():
    print("Registering fsspec known implementations and overriding all default implementations for persistence.")
    for k, v in known_implementations.items():
        DataPersistencePlugins.register_plugin(f"{k}://", FSSpecPersistence, force=True)


# Registering all plugins
_register()

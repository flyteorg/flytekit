import os
import typing

import fsspec
from fsspec.core import split_protocol
from fsspec.registry import known_implementations

from flytekit.configuration import aws as _aws_config
from flytekit.extend import DataPersistence, DataPersistencePlugins


def s3_setup_args():
    kwargs = {}
    if _aws_config.S3_ACCESS_KEY_ID.get() is not None:
        os.environ[_aws_config.S3_ACCESS_KEY_ID_ENV_NAME] = _aws_config.S3_ACCESS_KEY_ID.get()

    if _aws_config.S3_SECRET_ACCESS_KEY.get() is not None:
        os.environ[_aws_config.S3_SECRET_ACCESS_KEY_ENV_NAME] = _aws_config.S3_SECRET_ACCESS_KEY.get()

    # S3fs takes this as a special arg
    if _aws_config.S3_ENDPOINT.get() is not None:
        kwargs["client_kwargs"] = {"endpoint_url": _aws_config.S3_ENDPOINT.get()}

    return kwargs


class FSSpecPersistence(DataPersistence):
    def __init__(self, default_prefix=None):
        super(FSSpecPersistence, self).__init__(name="fsspec-persistence", default_prefix=default_prefix)
        self.default_protocol = self._get_protocol(default_prefix)

    @staticmethod
    def _get_protocol(path: typing.Optional[str] = None):
        if path:
            protocol, _ = split_protocol(path)
            if protocol is None and path.startswith("/"):
                print("Setting protocol to file")
                protocol = "file"
        else:
            protocol = "file"
        return protocol

    @staticmethod
    def _get_filesystem(path: str) -> fsspec.AbstractFileSystem:
        protocol = FSSpecPersistence._get_protocol(path)
        kwargs = {}
        if protocol == "file":
            kwargs = {"auto_mkdir": True}
        if protocol == "s3":
            kwargs = s3_setup_args()
        return fsspec.filesystem(protocol, **kwargs)

    @staticmethod
    def recursive_paths(p: str) -> str:
        if not p.endswith("*"):
            return os.path.join(p, "*")
        return p

    def exists(self, path: str) -> bool:
        print("FSSPEC Exists")
        fs = self._get_filesystem(path)
        return fs.exists(path)

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        print(f"FSSPEC get - {from_path} {to_path}")
        fs = self._get_filesystem(from_path)
        if recursive:
            to_path = self.recursive_paths(to_path)
            from_path = self.recursive_paths(from_path)
        return fs.get(from_path, to_path, recursive=recursive)

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self._get_filesystem(to_path)
        if recursive:
            to_path = self.recursive_paths(to_path)
            from_path = self.recursive_paths(from_path)
        print(f"FSSPEC put - {from_path} -> {to_path}, {recursive}")
        return fs.put(from_path, to_path, recursive=recursive)

    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths) -> str:
        paths = list(paths)  # make type check happy
        if add_prefix:
            paths = paths.insert(0, self.default_prefix)
        path = f"{'/'.join(paths)}"
        if add_protocol:
            return f"{self.default_protocol}://{path}"
        return path


def _register():
    print("Registering fsspec known implementations and overriding all default implementations for persistence.")
    DataPersistencePlugins.register_plugin("/", FSSpecPersistence, force=True)
    for k, v in known_implementations.items():
        DataPersistencePlugins.register_plugin(f"{k}://", FSSpecPersistence, force=True)


# Registering all plugins
_register()

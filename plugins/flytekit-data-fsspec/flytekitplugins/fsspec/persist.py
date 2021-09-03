import os
import typing

import fsspec
from fsspec.core import split_protocol
from fsspec.registry import known_implementations

from flytekit.configuration import aws as _aws_config
from flytekit.extend import DataPersistence, DataPersistencePlugins
from flytekit.loggers import logger


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
    """
    This DataPersistence plugin uses fsspec to perform the IO.
    NOTE: The put is not as performant as it can be for multiple files because of -
    https://github.com/intake/filesystem_spec/issues/724. Once this bug is fixed, we can remove the `HACK` in the put
    method
    """

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
        return fsspec.filesystem(protocol, **kwargs)  # type: ignore

    @staticmethod
    def recursive_paths(f: str, t: str) -> typing.Tuple[str, str]:
        if not f.endswith("*"):
            f = os.path.join(f, "*")
        if not t.endswith("/"):
            t += "/"
        return f, t

    def exists(self, path: str) -> bool:
        fs = self._get_filesystem(path)
        return fs.exists(path)

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self._get_filesystem(from_path)
        if recursive:
            from_path, to_path = self.recursive_paths(from_path, to_path)
        return fs.get(from_path, to_path, recursive=recursive)

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self._get_filesystem(to_path)
        if recursive:
            from_path, to_path = self.recursive_paths(from_path, to_path)
            # BEGIN HACK!
            # Once https://github.com/intake/filesystem_spec/issues/724 is fixed, delete the special recursive handling
            from fsspec.implementations.local import LocalFileSystem
            from fsspec.utils import other_paths

            lfs = LocalFileSystem()
            lpaths = lfs.expand_path(from_path, recursive=recursive)
            rpaths = other_paths(lpaths, to_path)
            for l, r in zip(lpaths, rpaths):
                fs.put_file(l, r)
            return
            # END OF HACK!!
        return fs.put(from_path, to_path, recursive=recursive)

    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths) -> str:
        path_list = list(paths)  # make type check happy
        if add_prefix:
            path_list.insert(0, self.default_prefix)  # type: ignore
        path = "/".join(path_list)
        if add_protocol:
            return f"{self.default_protocol}://{path}"
        return typing.cast(str, path)


def _register():
    logger.info("Registering fsspec known implementations and overriding all default implementations for persistence.")
    DataPersistencePlugins.register_plugin("/", FSSpecPersistence, force=True)
    for k, v in known_implementations.items():
        DataPersistencePlugins.register_plugin(f"{k}://", FSSpecPersistence, force=True)


# Registering all plugins
_register()

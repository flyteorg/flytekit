import os
import typing

import fsspec
from fsspec.registry import known_implementations

from flytekit.configuration import DataConfig, S3Config
from flytekit.extend import DataPersistence, DataPersistencePlugins
from flytekit.loggers import logger

S3_ACCESS_KEY_ID_ENV_NAME = "AWS_ACCESS_KEY_ID"
S3_SECRET_ACCESS_KEY_ENV_NAME = "AWS_SECRET_ACCESS_KEY"

# Refer to https://github.com/fsspec/s3fs/blob/50bafe4d8766c3b2a4e1fc09669cf02fb2d71454/s3fs/core.py#L198
# for key and secret
_FSSPEC_S3_KEY_ID = "key"
_FSSPEC_S3_SECRET = "secret"


def s3_setup_args(s3_cfg: S3Config):
    kwargs = {}
    if S3_ACCESS_KEY_ID_ENV_NAME not in os.environ:
        if s3_cfg.access_key_id:
            kwargs[_FSSPEC_S3_KEY_ID] = s3_cfg.access_key_id

    if S3_SECRET_ACCESS_KEY_ENV_NAME not in os.environ:
        if s3_cfg.secret_access_key:
            kwargs[_FSSPEC_S3_SECRET] = s3_cfg.secret_access_key

    # S3fs takes this as a special arg
    if s3_cfg.endpoint is not None:
        kwargs["client_kwargs"] = {"endpoint_url": s3_cfg.endpoint}

    return kwargs


class FSSpecPersistence(DataPersistence):
    """
    This DataPersistence plugin uses fsspec to perform the IO.
    NOTE: The put is not as performant as it can be for multiple files because of -
    https://github.com/intake/filesystem_spec/issues/724. Once this bug is fixed, we can remove the `HACK` in the put
    method
    """

    def __init__(self, default_prefix=None, data_config: typing.Optional[DataConfig] = None):
        super(FSSpecPersistence, self).__init__(name="fsspec-persistence", default_prefix=default_prefix)
        self.default_protocol = self.get_protocol(default_prefix)
        self._data_cfg = data_config if data_config else DataConfig.auto()

    @staticmethod
    def get_protocol(path: typing.Optional[str] = None):
        if path:
            return DataPersistencePlugins.get_protocol(path)
        logger.info("Setting protocol to file")
        return "file"

    def get_filesystem(self, path: str) -> fsspec.AbstractFileSystem:
        protocol = FSSpecPersistence.get_protocol(path)
        kwargs = {}
        if protocol == "file":
            kwargs = {"auto_mkdir": True}
        elif protocol == "s3":
            kwargs = s3_setup_args(self._data_cfg.s3)
        return fsspec.filesystem(protocol, **kwargs)  # type: ignore

    def get_anonymous_filesystem(self, path: str) -> typing.Optional[fsspec.AbstractFileSystem]:
        protocol = FSSpecPersistence.get_protocol(path)
        if protocol == "s3":
            kwargs = s3_setup_args(self._data_cfg.s3)
            anonymous_fs = fsspec.filesystem(protocol, anon=True, **kwargs)  # type: ignore
            return anonymous_fs
        return None

    @staticmethod
    def recursive_paths(f: str, t: str) -> typing.Tuple[str, str]:
        if not f.endswith("*"):
            f = os.path.join(f, "*")
        if not t.endswith("/"):
            t += "/"
        return f, t

    def exists(self, path: str) -> bool:
        try:
            fs = self.get_filesystem(path)
            return fs.exists(path)
        except OSError as oe:
            logger.debug(f"Error in exists checking {path} {oe}")
            fs = self.get_anonymous_filesystem(path)
            if fs is not None:
                logger.debug("S3 source detected, attempting anonymous S3 exists check")
                return fs.exists(path)
            raise oe

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self.get_filesystem(from_path)
        if recursive:
            from_path, to_path = self.recursive_paths(from_path, to_path)
        try:
            return fs.get(from_path, to_path, recursive=recursive)
        except OSError as oe:
            logger.debug(f"Error in getting {from_path} to {to_path} rec {recursive} {oe}")
            fs = self.get_anonymous_filesystem(from_path)
            if fs is not None:
                logger.debug("S3 source detected, attempting anonymous S3 access")
                return fs.get(from_path, to_path, recursive=recursive)
            raise oe

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        fs = self.get_filesystem(to_path)
        if recursive:
            from_path, to_path = self.recursive_paths(from_path, to_path)
            # BEGIN HACK!
            # Once https://github.com/intake/filesystem_spec/issues/724 is fixed, delete the special recursive handling
            from fsspec.implementations.local import LocalFileSystem
            from fsspec.utils import other_paths

            lfs = LocalFileSystem()
            try:
                lpaths = lfs.expand_path(from_path, recursive=recursive)
            except FileNotFoundError:
                # In some cases, there is no file in the original directory, so we just skip copying the file to the remote path
                logger.debug(f"there is no file in the {from_path}")
                return
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

"""
======================================
:mod:`flytekit.core.data_persistence`
======================================

.. currentmodule:: flytekit.core.data_persistence

The Data persistence module is used by core flytekit and most of the core TypeTransformers to manage data fetch & store,
between the durable backend store and the runtime environment. This is designed to be a pluggable system, with a default
simple implementation that ships with the core.

.. autosummary::
   :toctree: generated/
   :template: custom.rst
   :nosignatures:

   FileAccessProvider

"""
import os
import pathlib
import tempfile
import typing
from typing import Any, Dict, Union, cast
from uuid import UUID

import fsspec
from fsspec.utils import get_protocol

from flytekit import configuration
from flytekit.configuration import DataConfig
from flytekit.core.utils import timeit
from flytekit.exceptions.user import FlyteAssertion
from flytekit.interfaces.random import random
from flytekit.loggers import logger

# Refer to https://github.com/fsspec/s3fs/blob/50bafe4d8766c3b2a4e1fc09669cf02fb2d71454/s3fs/core.py#L198
# for key and secret
_FSSPEC_S3_KEY_ID = "key"
_FSSPEC_S3_SECRET = "secret"
_ANON = "anon"


def s3_setup_args(s3_cfg: configuration.S3Config, anonymous: bool = False):
    kwargs: Dict[str, Any] = {
        "cache_regions": True,
    }
    if s3_cfg.access_key_id:
        kwargs[_FSSPEC_S3_KEY_ID] = s3_cfg.access_key_id

    if s3_cfg.secret_access_key:
        kwargs[_FSSPEC_S3_SECRET] = s3_cfg.secret_access_key

    # S3fs takes this as a special arg
    if s3_cfg.endpoint is not None:
        kwargs["client_kwargs"] = {"endpoint_url": s3_cfg.endpoint}

    if anonymous:
        kwargs[_ANON] = True

    return kwargs


class FileAccessProvider(object):
    """
    This is the class that is available through the FlyteContext and can be used for persisting data to the remote
    durable store.
    """

    def __init__(
        self,
        local_sandbox_dir: Union[str, os.PathLike],
        raw_output_prefix: str,
        data_config: typing.Optional[DataConfig] = None,
    ):
        """
        Args:
            local_sandbox_dir: A local temporary working directory, that should be used to store data
        """
        # Local access
        if local_sandbox_dir is None or local_sandbox_dir == "":
            raise ValueError("FileAccessProvider needs to be created with a valid local_sandbox_dir")
        local_sandbox_dir_appended = os.path.join(local_sandbox_dir, "local_flytekit")
        self._local_sandbox_dir = pathlib.Path(local_sandbox_dir_appended)
        self._local_sandbox_dir.mkdir(parents=True, exist_ok=True)
        self._local = fsspec.filesystem(None)

        self._data_config = data_config if data_config else DataConfig.auto()
        self._default_protocol = get_protocol(raw_output_prefix)
        self._default_remote = cast(fsspec.AbstractFileSystem, self.get_filesystem(self._default_protocol))
        if os.name == "nt" and raw_output_prefix.startswith("file://"):
            raise FlyteAssertion("Cannot use the file:// prefix on Windows.")
        self._raw_output_prefix = (
            raw_output_prefix
            if raw_output_prefix.endswith(self.sep(self._default_remote))
            else raw_output_prefix + self.sep(self._default_remote)
        )

    @property
    def raw_output_prefix(self) -> str:
        return self._raw_output_prefix

    @property
    def data_config(self) -> DataConfig:
        return self._data_config

    def get_filesystem(
        self, protocol: typing.Optional[str] = None, anonymous: bool = False, **kwargs
    ) -> typing.Optional[fsspec.AbstractFileSystem]:
        if not protocol:
            return self._default_remote
        if protocol == "file":
            kwargs["auto_mkdir"] = True
        elif protocol == "s3":
            s3kwargs = s3_setup_args(self._data_config.s3, anonymous=anonymous)
            s3kwargs.update(kwargs)
            return fsspec.filesystem(protocol, **s3kwargs)  # type: ignore
        elif protocol == "gs":
            if anonymous:
                kwargs["token"] = _ANON
            return fsspec.filesystem(protocol, **kwargs)  # type: ignore

        # Preserve old behavior of returning None for file systems that don't have an explicit anonymous option.
        if anonymous:
            return None

        return fsspec.filesystem(protocol, **kwargs)  # type: ignore

    def get_filesystem_for_path(self, path: str = "", anonymous: bool = False, **kwargs) -> fsspec.AbstractFileSystem:
        protocol = get_protocol(path)
        return self.get_filesystem(protocol, anonymous=anonymous, **kwargs)

    @staticmethod
    def is_remote(path: Union[str, os.PathLike]) -> bool:
        """
        Deprecated. Let's find a replacement
        """
        protocol = get_protocol(path)
        if protocol is None:
            return False
        return protocol != "file"

    @property
    def local_sandbox_dir(self) -> os.PathLike:
        """
        This is a context based temp dir.
        """
        return self._local_sandbox_dir

    @property
    def local_access(self) -> fsspec.AbstractFileSystem:
        return self._local

    @staticmethod
    def strip_file_header(path: str, trim_trailing_sep: bool = False) -> str:
        """
        Drops file:// if it exists from the file
        """
        if path.startswith("file://"):
            return path.replace("file://", "", 1)
        return path

    @staticmethod
    def recursive_paths(f: str, t: str) -> typing.Tuple[str, str]:
        f = os.path.join(f, "")
        t = os.path.join(t, "")
        return f, t

    def sep(self, file_system: typing.Optional[fsspec.AbstractFileSystem]) -> str:
        if file_system is None or file_system.protocol == "file":
            return os.sep
        return file_system.sep

    def exists(self, path: str) -> bool:
        try:
            file_system = self.get_filesystem_for_path(path)
            return file_system.exists(path)
        except OSError as oe:
            logger.debug(f"Error in exists checking {path} {oe}")
            anon_fs = self.get_filesystem(get_protocol(path), anonymous=True)
            if anon_fs is not None:
                logger.debug(f"Attempting anonymous exists with {anon_fs}")
                return anon_fs.exists(path)
            raise oe

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        file_system = self.get_filesystem_for_path(from_path)
        if recursive:
            from_path, to_path = self.recursive_paths(from_path, to_path)
        try:
            if os.name == "nt" and file_system.protocol == "file" and recursive:
                import shutil

                return shutil.copytree(
                    self.strip_file_header(from_path), self.strip_file_header(to_path), dirs_exist_ok=True
                )
            return file_system.get(from_path, to_path, recursive=recursive)
        except OSError as oe:
            logger.debug(f"Error in getting {from_path} to {to_path} rec {recursive} {oe}")
            file_system = self.get_filesystem(get_protocol(from_path), anonymous=True)
            if file_system is not None:
                logger.debug(f"Attempting anonymous get with {file_system}")
                return file_system.get(from_path, to_path, recursive=recursive)
            raise oe

    def put(self, from_path: str, to_path: str, recursive: bool = False, **kwargs):
        file_system = self.get_filesystem_for_path(to_path)
        from_path = self.strip_file_header(from_path)
        if recursive:
            # Only check this for the local filesystem
            if file_system.protocol == "file" and not file_system.isdir(from_path):
                raise FlyteAssertion(f"Source path {from_path} is not a directory")
            if os.name == "nt" and file_system.protocol == "file":
                import shutil

                return shutil.copytree(
                    self.strip_file_header(from_path), self.strip_file_header(to_path), dirs_exist_ok=True
                )
            from_path, to_path = self.recursive_paths(from_path, to_path)
        return file_system.put(from_path, to_path, recursive=recursive, **kwargs)

    def get_random_remote_path(self, file_path_or_file_name: typing.Optional[str] = None) -> str:
        """
        Constructs a randomized path on the configured raw_output_prefix (persistence layer). the random bit is a UUID
        and allows for disambiguating paths within the same directory.

        Use file_path_or_file_name, when you want a random directory, but want to preserve the leaf file name
        """
        default_protocol = self._default_remote.protocol
        if type(default_protocol) == list:
            default_protocol = default_protocol[0]
        key = UUID(int=random.getrandbits(128)).hex
        tail = ""
        if file_path_or_file_name:
            _, tail = os.path.split(file_path_or_file_name)
        sep = self.sep(self._default_remote)
        tail = sep + tail if tail else tail
        if default_protocol == "file":
            # Special case the local case, users will not expect to see a file:// prefix
            return self.strip_file_header(self.raw_output_prefix) + key + tail

        return self._default_remote.unstrip_protocol(self.raw_output_prefix + key + tail)

    def get_random_remote_directory(self):
        return self.get_random_remote_path(None)

    def get_random_local_path(self, file_path_or_file_name: typing.Optional[str] = None) -> str:
        """
        Use file_path_or_file_name, when you want a random directory, but want to preserve the leaf file name
        """
        key = UUID(int=random.getrandbits(128)).hex
        tail = ""
        if file_path_or_file_name:
            _, tail = os.path.split(file_path_or_file_name)
        if tail:
            return os.path.join(self._local_sandbox_dir, key, tail)
        return os.path.join(self._local_sandbox_dir, key)

    def get_random_local_directory(self) -> str:
        _dir = self.get_random_local_path(None)
        pathlib.Path(_dir).mkdir(parents=True, exist_ok=True)
        return _dir

    def download_directory(self, remote_path: str, local_path: str):
        """
        Downloads directory from given remote to local path
        """
        return self.get_data(remote_path, local_path, is_multipart=True)

    def download(self, remote_path: str, local_path: str):
        """
        Downloads from remote to local
        """
        return self.get_data(remote_path, local_path)

    def upload(self, file_path: str, to_path: str):
        """
        :param Text file_path:
        :param Text to_path:
        """
        return self.put_data(file_path, to_path)

    def upload_directory(self, local_path: str, remote_path: str):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        return self.put_data(local_path, remote_path, is_multipart=True)

    def get_data(self, remote_path: str, local_path: str, is_multipart: bool = False):
        """
        :param remote_path:
        :param local_path:
        :param is_multipart:
        """
        try:
            pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with timeit(f"Download data to local from {remote_path}"):
                self.get(remote_path, to_path=local_path, recursive=is_multipart)
        except Exception as ex:
            raise FlyteAssertion(
                f"Failed to get data from {remote_path} to {local_path} (recursive={is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            )

    def put_data(self, local_path: Union[str, os.PathLike], remote_path: str, is_multipart: bool = False, **kwargs):
        """
        The implication here is that we're always going to put data to the remote location, so we .remote to ensure
        we don't use the true local proxy if the remote path is a file://

        :param local_path:
        :param remote_path:
        :param is_multipart:
        """
        try:
            local_path = str(local_path)
            with timeit(f"Upload data to {remote_path}"):
                self.put(cast(str, local_path), remote_path, recursive=is_multipart, **kwargs)
        except Exception as ex:
            raise FlyteAssertion(
                f"Failed to put data from {local_path} to {remote_path} (recursive={is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            ) from ex


flyte_tmp_dir = tempfile.mkdtemp(prefix="flyte-")
default_local_file_access_provider = FileAccessProvider(
    local_sandbox_dir=os.path.join(flyte_tmp_dir, "sandbox"),
    raw_output_prefix=os.path.join(flyte_tmp_dir, "raw"),
    data_config=DataConfig.auto(),
)

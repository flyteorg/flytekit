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

   DataPersistence
   DataPersistencePlugins
   DiskPersistence
   FileAccessProvider
   UnsupportedPersistenceOp

"""

import os
import pathlib
import re
import shutil
import sys
import tempfile
import typing
from abc import abstractmethod
from shutil import copyfile
from typing import Dict, Union
from uuid import UUID

import fsspec
from fsspec.utils import get_protocol

from flytekit import configuration
from flytekit.configuration import DataConfig
from flytekit.core.utils import PerformanceTimer
from flytekit.exceptions.user import FlyteAssertion, FlyteValueException
from flytekit.interfaces.random import random
from flytekit.loggers import logger

S3_ACCESS_KEY_ID_ENV_NAME = "AWS_ACCESS_KEY_ID"
S3_SECRET_ACCESS_KEY_ENV_NAME = "AWS_SECRET_ACCESS_KEY"

# Refer to https://github.com/fsspec/s3fs/blob/50bafe4d8766c3b2a4e1fc09669cf02fb2d71454/s3fs/core.py#L198
# for key and secret
_FSSPEC_S3_KEY_ID = "key"
_FSSPEC_S3_SECRET = "secret"


def s3_setup_args(s3_cfg: configuration.S3Config):
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

        self._raw_output_prefix = raw_output_prefix
        self._default_protocol = self.get_protocol(self._raw_output_prefix)
        self._default_remote = self.get_filesystem(self._default_protocol)
        self._data_config = data_config if data_config else DataConfig.auto()

    @property
    def raw_output_prefix(self) -> str:
        return self._raw_output_prefix

    @property
    def data_config(self) -> DataConfig:
        return self._data_config

    @staticmethod
    def get_protocol(path: typing.Optional[str] = None):
        if path:
            return get_protocol(path)
        logger.info("Setting protocol to file")
        return "file"

    def get_filesystem(self, protocol: str = None) -> fsspec.AbstractFileSystem:
        if not protocol:
            return self._default_remote
        kwargs = {}
        if protocol == "file":
            kwargs = {"auto_mkdir": True}
        elif protocol == "s3":
            kwargs = s3_setup_args(self._data_cfg.s3)
        return fsspec.filesystem(protocol, **kwargs)  # type: ignore

    def get_filesystem_for_path(self, path: str) -> fsspec.AbstractFileSystem:
        protocol = self.get_protocol(path)
        return self.get_filesystem(protocol)

    def get_anonymous_filesystem(self, path: str) -> typing.Optional[fsspec.AbstractFileSystem]:
        protocol = self.get_protocol(path)
        if protocol == "s3":
            kwargs = s3_setup_args(self._data_cfg.s3)
            anonymous_fs = fsspec.filesystem(protocol, anon=True, **kwargs)  # type: ignore
            return anonymous_fs
        return None

    @staticmethod
    def is_remote(path: Union[str, os.PathLike]) -> bool:
        """
        Deprecated. Lets find a replacement
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
        return fs.put(from_path, to_path, recursive=recursive)

    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths) -> str:
        path_list = list(paths)  # make type check happy
        if add_prefix:
            path_list.insert(0, self.default_prefix)  # type: ignore
        path = "/".join(path_list)
        if add_protocol:
            return f"{self._default_protocol}://{path}"
        return typing.cast(str, path)

    def construct_random_path(self, file_path_or_file_name: typing.Optional[str] = None) -> str:
        """
        Use file_path_or_file_name, when you want a random directory, but want to preserve the leaf file name
        """
        key = UUID(int=random.getrandbits(128)).hex
        if file_path_or_file_name:
            _, tail = os.path.split(file_path_or_file_name)
            if tail:
                return self.construct_path(False, True, key, tail)
            else:
                logger.warning(f"No filename detected in {file_path_or_file_name}, generating random path")
        return self.construct_path(False, True, key)

    def get_random_remote_path(self, file_path_or_file_name: typing.Optional[str] = None) -> str:
        """
        Constructs a randomized path on the configured raw_output_prefix (persistence layer). the random bit is a UUID
        and allows for disambiguating paths within the same directory.

        Use file_path_or_file_name, when you want a random directory, but want to preserve the leaf file name
        """
        return self.construct_random_path(self._default_remote, file_path_or_file_name)

    def get_random_remote_directory(self):
        return self.get_random_remote_path(None)

    def get_random_local_path(self, file_path_or_file_name: typing.Optional[str] = None) -> str:
        """
        Use file_path_or_file_name, when you want a random directory, but want to preserve the leaf file name
        """
        return self.construct_random_path(self._local, file_path_or_file_name)

    def get_random_local_directory(self) -> str:
        _dir = self.get_random_local_path(None)
        pathlib.Path(_dir).mkdir(parents=True, exist_ok=True)
        return _dir

    def exists(self, path: str) -> bool:
        """
        checks if the given path exists
        """
        return self.exists(path)

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

    def get_data(self, remote_path: str, local_path: str, is_multipart=False):
        """
        :param Text remote_path:
        :param Text local_path:
        :param bool is_multipart:
        """
        try:
            with PerformanceTimer(f"Copying ({remote_path} -> {local_path})"):
                pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                self.get(remote_path, to_path=local_path, recursive=is_multipart)
        except Exception as ex:
            raise FlyteAssertion(
                f"Failed to get data from {remote_path} to {local_path} (recursive={is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            )

    def put_data(self, local_path: Union[str, os.PathLike], remote_path: str, is_multipart=False):
        """
        The implication here is that we're always going to put data to the remote location, so we .remote to ensure
        we don't use the true local proxy if the remote path is a file://

        :param Text local_path:
        :param Text remote_path:
        :param bool is_multipart:
        """
        try:
            with PerformanceTimer(f"Writing ({local_path} -> {remote_path})"):
                self.put(local_path, remote_path, recursive=is_multipart)
        except Exception as ex:
            raise FlyteAssertion(
                f"Failed to put data from {local_path} to {remote_path} (recursive={is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            ) from ex


fsspec.register_implementation("/", )
DataPersistencePlugins.register_plugin("file://", DiskPersistence)
DataPersistencePlugins.register_plugin("/", DiskPersistence)

flyte_tmp_dir = tempfile.mkdtemp(prefix="flyte-")
default_local_file_access_provider = FileAccessProvider(
    local_sandbox_dir=os.path.join(flyte_tmp_dir, "sandbox"),
    raw_output_prefix=os.path.join(flyte_tmp_dir, "raw"),
    data_config=DataConfig.auto(),
)

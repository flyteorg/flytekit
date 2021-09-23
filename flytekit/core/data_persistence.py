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

import datetime
import os
import pathlib
import typing
from abc import abstractmethod
from distutils import dir_util
from shutil import copyfile
from typing import Dict, Union
from uuid import UUID

from flytekit.common.exceptions.user import FlyteAssertion
from flytekit.common.utils import PerformanceTimer
from flytekit.interfaces.random import random
from flytekit.loggers import logger


class UnsupportedPersistenceOp(Exception):
    """
    This exception is raised for all methods when a method is not supported by the data persistence layer
    """

    def __init__(self, message: str):
        super(UnsupportedPersistenceOp, self).__init__(message)


class DataPersistence(object):
    """
    Base abstract type for all DataPersistence operations. This can be extended using the flytekitplugins architecture
    """

    def __init__(self, name: str, default_prefix: typing.Optional[str] = None, **kwargs):
        self._name = name
        self._default_prefix = default_prefix

    @property
    def name(self) -> str:
        return self._name

    @property
    def default_prefix(self) -> typing.Optional[str]:
        return self._default_prefix

    def listdir(self, path: str, recursive: bool = False) -> typing.Generator[str, None, None]:
        """
        Returns true if the given path exists, else false
        """
        raise UnsupportedPersistenceOp(f"Listing a directory is not supported by the persistence plugin {self.name}")

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Returns true if the given path exists, else false
        """
        pass

    @abstractmethod
    def get(self, from_path: str, to_path: str, recursive: bool = False):
        """
        Retrieves data from from_path and writes to the given to_path (to_path is locally accessible)
        """
        pass

    @abstractmethod
    def put(self, from_path: str, to_path: str, recursive: bool = False):
        """
        Stores data from from_path and writes to the given to_path (from_path is locally accessible)
        """
        pass

    @abstractmethod
    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths: str) -> str:
        """
        if add_protocol is true then <protocol> is prefixed else
        Constructs a path in the format <base><delim>*args
        delim is dependent on the storage medium.
        each of the args is joined with the delim
        """
        pass


class DataPersistencePlugins(object):
    """
    DataPersistencePlugins is the core plugin registry that stores all DataPersistence plugins. To add a new plugin use

    .. code-block:: python

       DataPersistencePlugins.register_plugin("s3:/", DataPersistence(), force=True|False)

    These plugins should always be registered. Follow the plugin registration guidelines to auto-discover your plugins.
    """

    _PLUGINS: Dict[str, typing.Type[DataPersistence]] = {}

    @classmethod
    def register_plugin(cls, protocol: str, plugin: typing.Type[DataPersistence], force: bool = False):
        """
        Registers the supplied plugin for the specified protocol if one does not already exist.
        If one exists and force is default or False, then a TypeError is raised.
        If one does not exist then it is registered
        If one exists, but force == True then the existing plugin is overridden
        """
        if protocol in cls._PLUGINS:
            p = cls._PLUGINS[protocol]
            if p == plugin:
                return
            if not force:
                raise TypeError(
                    f"Cannot register plugin {plugin.name} for protocol {protocol} as plugin {p.name} is already"
                    f" registered for the same protocol. You can force register the new plugin by passing force=True"
                )

        cls._PLUGINS[protocol] = plugin

    @classmethod
    def find_plugin(cls, path: str) -> typing.Type[DataPersistence]:
        """
        Returns a plugin for the given protocol, else raise a TypeError
        """
        for k, p in cls._PLUGINS.items():
            if path.startswith(k):
                return p
        raise TypeError(f"No plugin found for matching protocol of path {path}")

    @classmethod
    def print_all_plugins(cls):
        """
        Prints all the plugins and their associated protocoles
        """
        for k, p in cls._PLUGINS.items():
            print(f"Plugin {p.name} registered for protocol {k}")

    @classmethod
    def is_supported_protocol(cls, protocol: str) -> bool:
        """
        Returns true if the given protocol is has a registered plugin for it
        """
        return protocol in cls._PLUGINS


class DiskPersistence(DataPersistence):
    """
    The simplest form of persistence that is available with default flytekit - Disk-based persistence.
    This will store all data locally and retrieve the data from local. This is helpful for local execution and simulating
    runs.
    """

    PROTOCOL = "file://"

    def __init__(self, default_prefix: typing.Optional[str] = None, **kwargs):
        super().__init__(name="local", default_prefix=default_prefix, **kwargs)

    @staticmethod
    def _make_local_path(path):
        if not os.path.exists(path):
            try:
                pathlib.Path(path).mkdir(parents=True, exist_ok=True)
            except OSError:  # Guard against race condition
                if not os.path.isdir(path):
                    raise

    @staticmethod
    def strip_file_header(path: str) -> str:
        """
        Drops file:// if it exists from the file
        """
        if path.startswith("file://"):
            return path.replace("file://", "", 1)
        return path

    def listdir(self, path: str, recursive: bool = False) -> typing.Generator[str, None, None]:
        if not recursive:
            files = os.listdir(self.strip_file_header(path))
            for f in files:
                yield f
            return

        for root, subdirs, files in os.walk(self.strip_file_header(path)):
            for f in files:
                yield os.path.join(root, f)
        return

    def exists(self, path: str):
        return os.path.exists(self.strip_file_header(path))

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        if from_path != to_path:
            if recursive:
                dir_util.copy_tree(self.strip_file_header(from_path), self.strip_file_header(to_path))
            else:
                copyfile(self.strip_file_header(from_path), self.strip_file_header(to_path))

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        if from_path != to_path:
            if recursive:
                dir_util.copy_tree(self.strip_file_header(from_path), self.strip_file_header(to_path))
            else:
                # Emulate s3's flat storage by automatically creating directory path
                self._make_local_path(os.path.dirname(self.strip_file_header(to_path)))
                # Write the object to a local file in the temp local folder
                copyfile(self.strip_file_header(from_path), self.strip_file_header(to_path))

    def construct_path(self, _: bool, add_prefix: bool, *args: str) -> str:
        # Ignore add_protocol for now. Only complicates things
        if add_prefix:
            prefix = self.default_prefix if self.default_prefix else ""
            return os.path.join(prefix, *args)
        return os.path.join(*args)


def stringify_path(filepath):
    """
    Copied from `filesystem_spec <https://github.com/intake/filesystem_spec/blob/master/fsspec/utils.py#L287:5>`__

    Attempt to convert a path-like object to a string.
    Parameters
    ----------
    filepath: object to be converted
    Returns
    -------
    filepath_str: maybe a string version of the object
    Notes
    -----
    Objects supporting the fspath protocol (Python 3.6+) are coerced
    according to its __fspath__ method.
    For backwards compatibility with older Python version, pathlib.Path
    objects are specially coerced.
    Any other object is passed through unchanged, which includes bytes,
    strings, buffers, or anything else that's not even path-like.
    """
    if isinstance(filepath, str):
        return filepath
    elif hasattr(filepath, "__fspath__"):
        return filepath.__fspath__()
    elif isinstance(filepath, pathlib.Path):
        return str(filepath)
    elif hasattr(filepath, "path"):
        return filepath.path
    else:
        return filepath


def split_protocol(urlpath):
    """
    Copied from `filesystem_spec <https://github.com/intake/filesystem_spec/blob/master/fsspec/core.py#L502>`__
    Return protocol, path pair
    """
    urlpath = stringify_path(urlpath)
    if "://" in urlpath:
        protocol, path = urlpath.split("://", 1)
        if len(protocol) > 1:
            # excludes Windows paths
            return protocol, path
    return None, urlpath


class FileAccessProvider(object):
    """
    This is the class that is available through the FlyteContext and can be used for persisting data to the remote
    durable store.
    """

    def __init__(self, local_sandbox_dir: Union[str, os.PathLike], raw_output_prefix: str):
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
        self._local = DiskPersistence(default_prefix=local_sandbox_dir_appended)

        self._default_remote = DataPersistencePlugins.find_plugin(raw_output_prefix)(default_prefix=raw_output_prefix)
        self._raw_output_prefix = raw_output_prefix

    @staticmethod
    def is_remote(path: Union[str, os.PathLike]) -> bool:
        """
        Deprecated. Lets find a replacement
        """
        protocol, _ = split_protocol(path)
        if protocol is None:
            return False
        return protocol != "file"

    @property
    def local_sandbox_dir(self) -> os.PathLike:
        return self._local_sandbox_dir

    @property
    def local_access(self) -> DiskPersistence:
        return self._local

    def construct_random_path(
        self, persist: DataPersistence, file_path_or_file_name: typing.Optional[str] = None
    ) -> str:
        """
        Use file_path_or_file_name, when you want a random directory, but want to preserve the leaf file name
        """
        key = UUID(int=random.getrandbits(128)).hex
        if file_path_or_file_name:
            _, tail = os.path.split(file_path_or_file_name)
            if tail:
                return persist.construct_path(False, True, key, tail)
            else:
                logger.warning(f"No filename detected in {file_path_or_file_name}, generating random path")
        return persist.construct_path(False, True, key)

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
        return DataPersistencePlugins.find_plugin(path)().exists(path)

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
                DataPersistencePlugins.find_plugin(remote_path)().get(remote_path, local_path, recursive=is_multipart)
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
                DataPersistencePlugins.find_plugin(remote_path)().put(local_path, remote_path, recursive=is_multipart)
        except Exception as ex:
            raise FlyteAssertion(
                f"Failed to put data from {local_path} to {remote_path} (recursive={is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            ) from ex


DataPersistencePlugins.register_plugin("file://", DiskPersistence)
DataPersistencePlugins.register_plugin("/", DiskPersistence)

# TODO make this use tmpdir
tmp_dir = os.path.join("/tmp/flyte", datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
default_local_file_access_provider = FileAccessProvider(
    local_sandbox_dir=os.path.join(tmp_dir, "sandbox"), raw_output_prefix=os.path.join(tmp_dir, "raw")
)

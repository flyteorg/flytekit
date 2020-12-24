import datetime
import os
import pathlib
from typing import Optional, Union

from flytekit.common import constants as _constants
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import user as _user_exception
from flytekit.configuration import platform as _platform_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy
from flytekit.interfaces.data.http import http_data_proxy as _http_data_proxy
from flytekit.interfaces.data.local import local_file_proxy as _local_file_proxy
from flytekit.interfaces.data.s3 import s3proxy as _s3proxy
from flytekit.loggers import logger


class LocalWorkingDirectoryContext(object):
    _CONTEXTS = []

    def __init__(self, directory):
        self._directory = directory

    def __enter__(self):
        self._CONTEXTS.append(self._directory)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._CONTEXTS.pop()

    @classmethod
    def get(cls):
        return cls._CONTEXTS[-1] if cls._CONTEXTS else None


class _OutputDataContext(object):
    _CONTEXTS = [_local_file_proxy.LocalFileProxy(_sdk_config.LOCAL_SANDBOX.get())]

    def __init__(self, context):
        self._context = context

    def __enter__(self):
        self._CONTEXTS.append(self._context)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._CONTEXTS.pop()

    @classmethod
    def get_active_proxy(cls):
        return cls._CONTEXTS[-1]

    @classmethod
    def get_default_proxy(cls):
        return cls._CONTEXTS[0]


class LocalDataContext(_OutputDataContext):
    def __init__(self, sandbox):
        """
        :param Text sandbox:
        """
        super(LocalDataContext, self).__init__(_local_file_proxy.LocalFileProxy(sandbox))


class RemoteDataContext(_OutputDataContext):
    _CLOUD_PROVIDER_TO_PROXIES = {
        _constants.CloudProvider.AWS: _s3proxy.AwsS3Proxy,
        _constants.CloudProvider.GCP: _gcs_proxy.GCSProxy,
    }

    def __init__(self, cloud_provider=None, raw_output_data_prefix_override=None):
        """
        :param Optional[Text] cloud_provider: From flytekit.common.constants.CloudProvider enum
        """
        cloud_provider = cloud_provider or _platform_config.CLOUD_PROVIDER.get()
        proxy_class = type(self)._CLOUD_PROVIDER_TO_PROXIES.get(cloud_provider, None)
        if proxy_class is None:
            raise _user_exception.FlyteAssertion(
                "Configured cloud provider is not supported for data I/O.  Received: {}, expected one of: {}".format(
                    cloud_provider, list(type(self)._CLOUD_PROVIDER_TO_PROXIES.keys())
                )
            )
        proxy = proxy_class(raw_output_data_prefix_override)
        super(RemoteDataContext, self).__init__(proxy)


class Data(object):
    # TODO: More proxies for more environments.
    _DATA_PROXIES = {
        "s3:/": _s3proxy.AwsS3Proxy(),
        "gs:/": _gcs_proxy.GCSProxy(),
        "http://": _http_data_proxy.HttpFileProxy(),
        "https://": _http_data_proxy.HttpFileProxy(),
    }

    @classmethod
    def _load_data_proxy_by_path(cls, path):
        """
        :param Text path:
        :rtype: flytekit.interfaces.data.common.DataProxy
        """
        for k, v in cls._DATA_PROXIES.items():
            if path.startswith(k):
                return v
        return _OutputDataContext.get_default_proxy()

    @classmethod
    def data_exists(cls, path):
        """
        :param Text path:
        :rtype: bool: whether the file exists or not
        """
        with _common_utils.PerformanceTimer("Check file exists {}".format(path)):
            proxy = cls._load_data_proxy_by_path(path)
            return proxy.exists(path)

    @classmethod
    def get_data(cls, remote_path, local_path, is_multipart=False):
        """
        :param Text remote_path:
        :param Text local_path:
        :param bool is_multipart:
        """
        try:
            with _common_utils.PerformanceTimer("Copying ({} -> {})".format(remote_path, local_path)):
                proxy = cls._load_data_proxy_by_path(remote_path)
                if is_multipart:
                    proxy.download_directory(remote_path, local_path)
                else:
                    proxy.download(remote_path, local_path)
        except Exception as ex:
            raise _user_exception.FlyteAssertion(
                "Failed to get data from {remote_path} to {local_path} (recursive={is_multipart}).\n\n"
                "Original exception: {error_string}".format(
                    remote_path=remote_path, local_path=local_path, is_multipart=is_multipart, error_string=str(ex),
                )
            )

    @classmethod
    def put_data(cls, local_path, remote_path, is_multipart=False):
        """
        :param Text local_path:
        :param Text remote_path:
        :param bool is_multipart:
        """
        try:
            with _common_utils.PerformanceTimer("Writing ({} -> {})".format(local_path, remote_path)):
                proxy = cls._load_data_proxy_by_path(remote_path)
                if is_multipart:
                    proxy.upload_directory(local_path, remote_path)
                else:
                    proxy.upload(local_path, remote_path)
        except Exception as ex:
            raise _user_exception.FlyteAssertion(
                "Failed to put data from {local_path} to {remote_path} (recursive={is_multipart}).\n\n"
                "Original exception: {error_string}".format(
                    remote_path=remote_path, local_path=local_path, is_multipart=is_multipart, error_string=str(ex),
                )
            )

    @classmethod
    def get_remote_path(cls):
        """
        :rtype: Text
        """
        return _OutputDataContext.get_active_proxy().get_random_path()

    @classmethod
    def get_remote_directory(cls):
        """
        :rtype: Text
        """
        return _OutputDataContext.get_active_proxy().get_random_directory()


class FileAccessProvider(object):
    def __init__(
        self,
        local_sandbox_dir: Union[str, os.PathLike],
        remote_proxy: Union[_s3proxy.AwsS3Proxy, _gcs_proxy.GCSProxy, None] = None,
    ):

        # Local access
        if local_sandbox_dir is None or local_sandbox_dir == "":
            raise Exception("Can't use empty path")
        local_sandbox_dir_appended = os.path.join(local_sandbox_dir, "local_flytekit")
        pathlib.Path(local_sandbox_dir_appended).mkdir(parents=True, exist_ok=True)
        self._local_sandbox_dir = local_sandbox_dir_appended
        self._local = _local_file_proxy.LocalFileProxy(local_sandbox_dir_appended)

        # Remote/cloud stuff
        if isinstance(remote_proxy, _s3proxy.AwsS3Proxy):
            self._aws = remote_proxy
        if isinstance(remote_proxy, _gcs_proxy.GCSProxy):
            self._gcs = remote_proxy
        if remote_proxy is not None:
            self._remote = remote_proxy
        else:
            mock_remote = os.path.join(local_sandbox_dir, "mock_remote")
            pathlib.Path(mock_remote).mkdir(parents=True, exist_ok=True)
            self._remote = _local_file_proxy.LocalFileProxy(mock_remote)

        # HTTP access
        self._http_proxy = _http_data_proxy.HttpFileProxy()

    @staticmethod
    def is_remote(path: Union[str, os.PathLike]) -> bool:
        if path.startswith("s3:/") or path.startswith("gs:/") or path.startswith("file:/") or path.startswith("http"):
            return True
        return False

    def _get_data_proxy_by_path(self, path: Union[str, os.PathLike]):
        """
        :param Text path:
        :rtype: flytekit.interfaces.data.common.DataProxy
        """
        if path.startswith("s3:/"):
            return self.aws
        elif path.startswith("gs:/"):
            return self.gcs
        elif path.startswith("http"):
            return self.http
        elif path.startswith("file://"):
            # Note that we default to the local one here, not the remote one.
            return self.local_access
        elif path.startswith("/"):
            # Note that we default to the local one here, not the remote one.
            return self.local_access
        raise Exception(f"Unknown file access {path}")

    @property
    def aws(self) -> _s3proxy.AwsS3Proxy:
        if self._aws is None:
            raise Exception("No AWS handler found")
        return self._aws

    @property
    def gcs(self) -> _gcs_proxy.GCSProxy:
        if self._gcs is None:
            raise Exception("No GCP handler found")
        return self._gcs

    @property
    def remote(self):
        if self._remote is not None:
            return self._remote
        raise Exception("No cloud provider specified")

    @property
    def http(self) -> _http_data_proxy.HttpFileProxy:
        return self._http_proxy

    @property
    def local_sandbox_dir(self) -> os.PathLike:
        return self._local_sandbox_dir

    @property
    def local_access(self) -> _local_file_proxy.LocalFileProxy:
        return self._local

    def get_random_remote_path(self, file_path_or_file_name: Optional[str] = None) -> str:
        """
        :param file_path_or_file_name: For when you want a random directory, but want to preserve the leaf file name
        """
        if file_path_or_file_name:
            _, tail = os.path.split(file_path_or_file_name)
            if tail:
                return f"{self.remote.get_random_directory()}{tail}"
            else:
                logger.warning(f"No filename detected in {file_path_or_file_name}, using random remote path...")
        return self.remote.get_random_path()

    def get_random_remote_directory(self):
        return self.remote.get_random_directory()

    def get_random_local_path(self, file_path_or_file_name: Optional[str] = None) -> str:
        """
        :param file_path_or_file_name:  For when you want a random directory, but want to preserve the leaf file name
        """
        if file_path_or_file_name:
            _, tail = os.path.split(file_path_or_file_name)
            if tail:
                return os.path.join(self.local_access.get_random_directory(), tail)
            else:
                logger.warning(f"No filename detected in {file_path_or_file_name}, using random local path...")
        return self.local_access.get_random_path()

    def get_random_local_directory(self) -> str:
        dir = self.local_access.get_random_directory()
        pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
        return dir

    def exists(self, remote_path: str) -> bool:
        """
        :param Text remote_path: remote s3:// or gs:// path
        """
        return self._get_data_proxy_by_path(remote_path).exists(remote_path)

    def download_directory(self, remote_path: str, local_path: str):
        """
        :param Text remote_path: remote s3:// path
        :param Text local_path: directory to copy to
        """
        return self._get_data_proxy_by_path(remote_path).download_directory(remote_path, local_path)

    def download(self, remote_path: str, local_path: str):
        """
        :param Text remote_path: remote s3:// path
        :param Text local_path: directory to copy to
        """
        return self._get_data_proxy_by_path(remote_path).download(remote_path, local_path)

    def upload(self, file_path: str, to_path: str):
        """
        :param Text file_path:
        :param Text to_path:
        """
        return self.remote.upload(file_path, to_path)

    def upload_directory(self, local_path: str, remote_path: str):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        return self.remote.upload_directory(local_path, remote_path)

    def get_data(self, remote_path: str, local_path: str, is_multipart=False):
        """
        :param Text remote_path:
        :param Text local_path:
        :param bool is_multipart:
        """
        try:
            with _common_utils.PerformanceTimer("Copying ({} -> {})".format(remote_path, local_path)):
                if is_multipart:
                    self.download_directory(remote_path, local_path)
                else:
                    self.download(remote_path, local_path)
        except Exception as ex:
            raise _user_exception.FlyteAssertion(
                "Failed to get data from {remote_path} to {local_path} (recursive={is_multipart}).\n\n"
                "Original exception: {error_string}".format(
                    remote_path=remote_path, local_path=local_path, is_multipart=is_multipart, error_string=str(ex),
                )
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
            with _common_utils.PerformanceTimer("Writing ({} -> {})".format(local_path, remote_path)):
                if is_multipart:
                    self.remote.upload_directory(local_path, remote_path)
                else:
                    self.remote.upload(local_path, remote_path)
        except Exception as ex:
            raise _user_exception.FlyteAssertion(
                f"Failed to put data from {local_path} to {remote_path} (recursive={is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            ) from ex


timestamped_default_sandbox_location = os.path.join(
    _sdk_config.LOCAL_SANDBOX.get(), datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
)
default_local_file_access_provider = FileAccessProvider(local_sandbox_dir=timestamped_default_sandbox_location)

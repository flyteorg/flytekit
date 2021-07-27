from flytekit.common import constants as _constants
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import user as _user_exception
from flytekit.configuration import platform as _platform_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy
from flytekit.interfaces.data.http import http_data_proxy as _http_data_proxy
from flytekit.interfaces.data.local import local_file_proxy as _local_file_proxy
from flytekit.interfaces.data.s3 import s3proxy as _s3proxy


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
                    remote_path=remote_path,
                    local_path=local_path,
                    is_multipart=is_multipart,
                    error_string=str(ex),
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
                    remote_path=remote_path,
                    local_path=local_path,
                    is_multipart=is_multipart,
                    error_string=str(ex),
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

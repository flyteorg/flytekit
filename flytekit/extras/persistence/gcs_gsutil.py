import os
import typing
from shutil import which as shell_which

from flytekit.common.exceptions.user import FlyteUserException
from flytekit.configuration import gcp
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins
from flytekit.tools import subprocess


def _update_cmd_config_and_execute(cmd):
    env = os.environ.copy()
    return subprocess.check_call(cmd, env=env)


def _amend_path(path):
    return os.path.join(path, "*") if not path.endswith("*") else path


class GCSPersistence(DataPersistence):
    """
    This DataPersistence plugin uses a preinstalled GSUtil binary in the container to download and upload data.

    The binary can be installed in multiple ways including simply,

    .. prompt::

       pip install gsutil

    """

    _GS_UTIL_CLI = "gsutil"
    PROTOCOL = "gs://"

    def __init__(self, default_prefix: typing.Optional[str] = None):
        super(GCSPersistence, self).__init__(name="gcs-gsutil", default_prefix=default_prefix)

    @staticmethod
    def _check_binary():
        """
        Make sure that the `gsutil` cli is present
        """
        if not shell_which(GCSPersistence._GS_UTIL_CLI):
            raise FlyteUserException("gsutil (gcloud cli) not found! Please install using `pip install gsutil`.")

    @staticmethod
    def _maybe_with_gsutil_parallelism(*gsutil_args):
        """
        Check if we should run `gsutil` with the `-m` flag that enables
        parallelism via multiple threads/processes. Additional tweaking of
        this behavior can be achieved via the .boto configuration file. See:
        https://cloud.google.com/storage/docs/boto-gsutil
        """
        cmd = [GCSPersistence._GS_UTIL_CLI]
        if gcp.GSUTIL_PARALLELISM.get():
            cmd.append("-m")
        cmd.extend(gsutil_args)

        return cmd

    def exists(self, remote_path):
        """
        :param Text remote_path: remote gs:// path
        :rtype bool: whether the gs file exists or not
        """
        GCSPersistence._check_binary()

        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        cmd = [GCSPersistence._GS_UTIL_CLI, "-q", "stat", remote_path]
        try:
            _update_cmd_config_and_execute(cmd)
            return True
        except Exception:
            return False

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        if not from_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        GCSPersistence._check_binary()
        if recursive:
            cmd = self._maybe_with_gsutil_parallelism("cp", "-r", _amend_path(from_path), to_path)
        else:
            cmd = self._maybe_with_gsutil_parallelism("cp", from_path, to_path)

        return _update_cmd_config_and_execute(cmd)

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        GCSPersistence._check_binary()

        if recursive:
            cmd = self._maybe_with_gsutil_parallelism(
                "cp",
                "-r",
                _amend_path(from_path),
                to_path if to_path.endswith("/") else to_path + "/",
            )
        else:
            cmd = self._maybe_with_gsutil_parallelism("cp", from_path, to_path)
        return _update_cmd_config_and_execute(cmd)

    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths) -> str:
        paths = list(paths)  # make type check happy
        if add_prefix:
            paths.insert(0, self.default_prefix)
        path = "/".join(paths)
        if add_protocol:
            return f"{self.PROTOCOL}{path}"
        return path


DataPersistencePlugins.register_plugin(GCSPersistence.PROTOCOL, GCSPersistence)

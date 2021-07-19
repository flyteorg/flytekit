import os as _os
import sys as _sys
import uuid as _uuid

from flytekit.common.exceptions.user import FlyteUserException as _FlyteUserException
from flytekit.configuration import gcp as _gcp_config
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins
from flytekit.interfaces import random as _flyte_random
from flytekit.tools import subprocess as _subprocess

if _sys.version_info >= (3,):
    from shutil import which as _which
else:
    from distutils.spawn import find_executable as _which


def _update_cmd_config_and_execute(cmd):
    env = _os.environ.copy()
    return _subprocess.check_call(cmd, env=env)


def _amend_path(path):
    return _os.path.join(path, "*") if not path.endswith("*") else path


class GCSPersistence(DataPersistence):
    _GS_UTIL_CLI = "gsutil"
    PROTOCOL = "gs://"

    def __init__(self, raw_output_data_prefix_override: str = None):
        """
        :param raw_output_data_prefix_override: Instead of relying on the AWS or GCS configuration (see
            S3_SHARD_FORMATTER for AWS and GCS_PREFIX for GCP) setting when computing the shard
            path (_get_shard_path), use this prefix instead as a base. This code assumes that the
            path passed in is correct. That is, an S3 path won't be passed in when running on GCP.
        """
        self._raw_output_data_prefix_override = raw_output_data_prefix_override
        super(GCSPersistence, self).__init__(name="gcs-gsutil")

    @property
    def raw_output_data_prefix_override(self) -> str:
        return self._raw_output_data_prefix_override

    @staticmethod
    def _check_binary():
        """
        Make sure that the `gsutil` cli is present
        """
        if not _which(GCSPersistence._GS_UTIL_CLI):
            raise _FlyteUserException("gsutil (gcloud cli) not found! Please install.")

    @staticmethod
    def _maybe_with_gsutil_parallelism(*gsutil_args):
        """
        Check if we should run `gsutil` with the `-m` flag that enables
        parallelism via multiple threads/processes. Additional tweaking of
        this behavior can be achieved via the .boto configuration file. See:
        https://cloud.google.com/storage/docs/boto-gsutil
        """
        cmd = [GCSPersistence._GS_UTIL_CLI]
        if _gcp_config.GSUTIL_PARALLELISM.get():
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

    def download_directory(self, remote_path, local_path):
        """
        :param Text remote_path: remote gs:// path
        :param Text local_path: directory to copy to
        """
        GCSPersistence._check_binary()

        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        cmd = self._maybe_with_gsutil_parallelism("cp", "-r", _amend_path(remote_path), local_path)
        return _update_cmd_config_and_execute(cmd)

    def download(self, remote_path, local_path):
        """
        :param Text remote_path: remote gs:// path
        :param Text local_path: directory to copy to
        """
        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        GCSPersistence._check_binary()

        cmd = self._maybe_with_gsutil_parallelism("cp", remote_path, local_path)
        return _update_cmd_config_and_execute(cmd)

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """
        GCSPersistence._check_binary()

        cmd = self._maybe_with_gsutil_parallelism("cp", file_path, to_path)
        return _update_cmd_config_and_execute(cmd)

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        GCSPersistence._check_binary()

        cmd = self._maybe_with_gsutil_parallelism(
            "cp",
            "-r",
            _amend_path(local_path),
            remote_path if remote_path.endswith("/") else remote_path + "/",
        )
        return _update_cmd_config_and_execute(cmd)

    def construct_path(self, add_protocol: bool, *paths) -> str:
        path = f"{'/'.join(paths)}"
        if add_protocol:
            return f"{self.PROTOCOL}{path}"
        return path


DataPersistencePlugins.register_plugin("gcs://", GCSPersistence())
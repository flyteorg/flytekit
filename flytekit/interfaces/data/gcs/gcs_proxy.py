import os as _os
import sys as _sys
import uuid as _uuid

from flytekit.common.exceptions.user import FlyteUserException as _FlyteUserException
from flytekit.configuration import gcp as _gcp_config
from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import common as _common_data
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


class GCSProxy(_common_data.DataProxy):
    _GS_UTIL_CLI = "gsutil"

    def __init__(self, raw_output_data_prefix_override: str = None):
        """
        :param raw_output_data_prefix_override: Instead of relying on the AWS or GCS configuration (see
            S3_SHARD_FORMATTER for AWS and GCS_PREFIX for GCP) setting when computing the shard
            path (_get_shard_path), use this prefix instead as a base. This code assumes that the
            path passed in is correct. That is, an S3 path won't be passed in when running on GCP.
        """
        self._raw_output_data_prefix_override = raw_output_data_prefix_override
        super(GCSProxy, self).__init__(name="gcs-gsutil")

    @property
    def raw_output_data_prefix_override(self) -> str:
        return self._raw_output_data_prefix_override

    @staticmethod
    def _check_binary():
        """
        Make sure that the `gsutil` cli is present
        """
        if not _which(GCSProxy._GS_UTIL_CLI):
            raise _FlyteUserException("gsutil (gcloud cli) not found! Please install.")

    @staticmethod
    def _maybe_with_gsutil_parallelism(*gsutil_args):
        """
        Check if we should run `gsutil` with the `-m` flag that enables
        parallelism via multiple threads/processes. Additional tweaking of
        this behavior can be achieved via the .boto configuration file. See:
        https://cloud.google.com/storage/docs/boto-gsutil
        """
        cmd = [GCSProxy._GS_UTIL_CLI]
        if _gcp_config.GSUTIL_PARALLELISM.get():
            cmd.append("-m")
        cmd.extend(gsutil_args)

        return cmd

    def exists(self, remote_path):
        """
        :param Text remote_path: remote gs:// path
        :rtype bool: whether the gs file exists or not
        """
        GCSProxy._check_binary()

        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        cmd = [GCSProxy._GS_UTIL_CLI, "-q", "stat", remote_path]
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
        GCSProxy._check_binary()

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

        GCSProxy._check_binary()

        cmd = self._maybe_with_gsutil_parallelism("cp", remote_path, local_path)
        return _update_cmd_config_and_execute(cmd)

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """
        GCSProxy._check_binary()

        cmd = self._maybe_with_gsutil_parallelism("cp", file_path, to_path)
        return _update_cmd_config_and_execute(cmd)

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        GCSProxy._check_binary()

        cmd = self._maybe_with_gsutil_parallelism(
            "cp",
            "-r",
            _amend_path(local_path),
            remote_path if remote_path.endswith("/") else remote_path + "/",
        )
        return _update_cmd_config_and_execute(cmd)

    def get_random_path(self) -> str:
        """
        If this object was created with a raw output data prefix, usually set by Propeller/Plugins at execution time
        and piped all the way here, it will be used instead of referencing the GCS_PREFIX configuration.
        """
        key = _uuid.UUID(int=_flyte_random.random.getrandbits(128)).hex
        prefix = self.raw_output_data_prefix_override or _gcp_config.GCS_PREFIX.get()
        return _os.path.join(prefix, key)

    def get_random_directory(self):
        """
        :rtype: Text
        """
        return self.get_random_path() + "/"

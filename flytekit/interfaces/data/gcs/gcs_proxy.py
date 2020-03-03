from __future__ import absolute_import

import os as _os
import sys as _sys
import uuid as _uuid

from flytekit.configuration import gcp as _gcp_config
from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import common as _common_data
from flytekit.tools import subprocess as _subprocess
from flytekit.common.exceptions.user import FlyteUserException as _FlyteUserException


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

    @staticmethod
    def _check_binary():
        """
        Make sure that the AWS cli is present
        """
        if not _which(GCSProxy._GS_UTIL_CLI):
            raise _FlyteUserException('gsutil (gcloud cli) not found at Please install.')

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

        cmd = [GCSProxy._GS_UTIL_CLI, "cp", "-r", _amend_path(remote_path), local_path]
        return _update_cmd_config_and_execute(cmd)

    def download(self, remote_path, local_path):
        """
        :param Text remote_path: remote gs:// path
        :param Text local_path: directory to copy to
        """
        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        GCSProxy._check_binary()
        cmd = [GCSProxy._GS_UTIL_CLI, "cp", remote_path, local_path]
        return _update_cmd_config_and_execute(cmd)

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """
        GCSProxy._check_binary()

        cmd = [GCSProxy._GS_UTIL_CLI, "cp", file_path, to_path]

        return _update_cmd_config_and_execute(cmd)

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        if not remote_path.startswith("gs://"):
            raise ValueError("Not an GS Key. Please use FQN (GS ARN) of the format gs://...")

        GCSProxy._check_binary()

        cmd = [GCSProxy._GS_UTIL_CLI,
               "cp",
               "-r",
               _amend_path(local_path),
               remote_path if remote_path.endswith("/") else remote_path + "/"]
        return _update_cmd_config_and_execute(cmd)

    def get_random_path(self):
        """
        :rtype: Text
        """
        key = _uuid.UUID(int=_flyte_random.random.getrandbits(128)).hex
        return _os.path.join(_gcp_config.GCS_PREFIX.get(), key)

    def get_random_directory(self):
        """
        :rtype: Text
        """
        return self.get_random_path() + "/"

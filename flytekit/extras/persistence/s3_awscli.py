import logging
import os as _os
import re as _re
import string as _string
import sys as _sys
import time
from typing import Dict, List

from six import moves as _six_moves
from six import text_type as _text_type

from flytekit.common.exceptions.user import FlyteUserException as _FlyteUserException
from flytekit.configuration import aws as _aws_config
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins
from flytekit.tools import subprocess as _subprocess

if _sys.version_info >= (3,):
    from shutil import which as _which
else:
    from distutils.spawn import find_executable as _which


def _update_cmd_config_and_execute(cmd: List[str]):
    env = _os.environ.copy()

    if _aws_config.ENABLE_DEBUG.get():
        cmd.insert(1, "--debug")

    if _aws_config.S3_ENDPOINT.get() is not None:
        cmd.insert(1, _aws_config.S3_ENDPOINT.get())
        cmd.insert(1, _aws_config.S3_ENDPOINT_ARG_NAME)

    if _aws_config.S3_ACCESS_KEY_ID.get() is not None:
        env[_aws_config.S3_ACCESS_KEY_ID_ENV_NAME] = _aws_config.S3_ACCESS_KEY_ID.get()

    if _aws_config.S3_SECRET_ACCESS_KEY.get() is not None:
        env[_aws_config.S3_SECRET_ACCESS_KEY_ENV_NAME] = _aws_config.S3_SECRET_ACCESS_KEY.get()

    retry = 0
    while True:
        try:
            return _subprocess.check_call(cmd, env=env)
        except Exception as e:
            logging.error(f"Exception when trying to execute {cmd}, reason: {str(e)}")
            retry += 1
            if retry > _aws_config.RETRIES.get():
                raise
            secs = _aws_config.BACKOFF_SECONDS.get()
            logging.info(f"Sleeping before retrying again, after {secs} seconds")
            time.sleep(secs)
            logging.info("Retrying again")


def _extra_args(extra_args: Dict[str, str]) -> List[str]:
    cmd = []
    if "ContentType" in extra_args:
        cmd += ["--content-type", extra_args["ContentType"]]
    if "ContentEncoding" in extra_args:
        cmd += ["--content-encoding", extra_args["ContentEncoding"]]
    if "ACL" in extra_args:
        cmd += ["--acl", extra_args["ACL"]]
    return cmd


class S3Persistence(DataPersistence):
    PROTOCOL = "s3://"
    _AWS_CLI = "aws"
    _SHARD_CHARACTERS = [_text_type(x) for x in _six_moves.range(10)] + list(_string.ascii_lowercase)

    def __init__(self):
        super().__init__(name="awscli-s3")

    @staticmethod
    def _check_binary():
        """
        Make sure that the AWS cli is present
        """
        if not _which(S3Persistence._AWS_CLI):
            raise _FlyteUserException("AWS CLI not found at Please install.")

    @staticmethod
    def _split_s3_path_to_bucket_and_key(path):
        """
        :param Text path:
        :rtype: (Text, Text)
        """
        path = path[len("s3://") :]
        first_slash = path.index("/")
        return path[:first_slash], path[first_slash + 1 :]

    def exists(self, remote_path):
        """
        :param Text remote_path: remote s3:// path
        :rtype bool: whether the s3 file exists or not
        """
        S3Persistence._check_binary()

        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        bucket, file_path = self._split_s3_path_to_bucket_and_key(remote_path)
        cmd = [
            S3Persistence._AWS_CLI,
            "s3api",
            "head-object",
            "--bucket",
            bucket,
            "--key",
            file_path,
        ]
        try:
            _update_cmd_config_and_execute(cmd)
            return True
        except Exception as ex:
            # The s3api command returns an error if the object does not exist. The error message contains
            # the http status code: "An error occurred (404) when calling the HeadObject operation: Not Found"
            #  This is a best effort for returning if the object does not exist by searching
            # for existence of (404) in the error message. This should not be needed when we get off the cli and use lib
            if _re.search("(404)", _text_type(ex)):
                return False
            else:
                raise ex

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        S3Persistence._check_binary()

        if not from_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        if recursive:
            cmd = [S3Persistence._AWS_CLI, "s3", "cp", "--recursive", from_path, to_path]
        else:
            cmd = [S3Persistence._AWS_CLI, "s3", "cp", remote_path, local_path]
        return _update_cmd_config_and_execute(cmd)

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        extra_args = {
            "ACL": "bucket-owner-full-control",
        }

        if not to_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        S3Persistence._check_binary()
        cmd = [S3Persistence._AWS_CLI, "s3", "cp"]
        if recursive:
            cmd += ["--recursive"]
        cmd.extend(_extra_args(extra_args))
        cmd += [from_path, to_path]
        return _update_cmd_config_and_execute(cmd)

    def construct_path(self, add_protocol: bool, *paths) -> str:
        path = f"{'/'.join(paths)}"
        if add_protocol:
            return f"{self.PROTOCOL}{path}"
        return path


DataPersistencePlugins.register_plugin("s3://", S3Persistence())

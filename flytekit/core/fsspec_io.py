import os
import typing

from flytekit.configuration import aws as _aws_config
from flytekit.core.data_persistence import split_protocol
from flytekit.loggers import logger

_fsspec_io = False


def s3_setup_args():
    if _aws_config.S3_ACCESS_KEY_ID.get() is not None:
        os.environ[_aws_config.S3_ACCESS_KEY_ID_ENV_NAME] = _aws_config.S3_ACCESS_KEY_ID.get()

    if _aws_config.S3_SECRET_ACCESS_KEY.get() is not None:
        os.environ[_aws_config.S3_SECRET_ACCESS_KEY_ENV_NAME] = _aws_config.S3_SECRET_ACCESS_KEY.get()

    if _aws_config.S3_ENDPOINT.get() is not None:
        os.environ[_aws_config.S3_ENDPOINT_KEY_ENV_NAME] = _aws_config.S3_ENDPOINT.get()


def get_client_kwargs() -> typing.Optional[typing.Dict]:
    if _fsspec_io and _aws_config.S3_ENDPOINT.get() is not None:
        return {"endpoint_url": _aws_config.S3_ENDPOINT.get()}
    return None


def get_protocol(path: typing.Optional[str] = None):
    if path:
        protocol, _ = split_protocol(path)
        if protocol is None and path.startswith("/"):
            logger.info("Setting protocol to file")
            protocol = "file"
    else:
        protocol = "file"
    return protocol


def enable_fssepc_io():
    global _fsspec_io
    s3_setup_args()
    _fsspec_io = True


def disable_fsspec_io():
    global _fsspec_io
    _fsspec_io = False


def is_fsspec_io() -> bool:
    return _fsspec_io

import os
import typing

from flytekit.configuration import aws as _aws_config
from flytekit.core.data_persistence import split_protocol


def get_filesystem(path: typing.Union[str, os.PathLike]) -> typing.Optional["fsspec.AbstractFileSystem"]:
    protocol, _ = split_protocol(path)
    if protocol == "s3":
        kwargs = _get_s3_config()
        import fsspec

        return fsspec.filesystem(protocol, **kwargs)  # type: ignore
    return None


def get_storage_config(path: typing.Union[str, os.PathLike]) -> dict:
    protocol, _ = split_protocol(path)
    if protocol == "s3":
        return _get_s3_config()
    return {}


def _get_s3_config() -> dict:
    kwargs = {}
    if _aws_config.S3_ACCESS_KEY_ID.get() is not None:
        os.environ[_aws_config.S3_ACCESS_KEY_ID_ENV_NAME] = _aws_config.S3_ACCESS_KEY_ID.get()

    if _aws_config.S3_SECRET_ACCESS_KEY.get() is not None:
        os.environ[_aws_config.S3_SECRET_ACCESS_KEY_ENV_NAME] = _aws_config.S3_SECRET_ACCESS_KEY.get()

    # S3fs takes this as a special arg
    if _aws_config.S3_ENDPOINT.get() is not None:
        kwargs["client_kwargs"] = {"endpoint_url": _aws_config.S3_ENDPOINT.get()}

    return kwargs

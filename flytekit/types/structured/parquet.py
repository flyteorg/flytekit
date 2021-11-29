import os
from typing import Any, Optional, TypeVar, Union

import pandas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from flytekit.configuration import aws as _aws_config
from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    DatasetDecodingHandler,
    DatasetEncodingHandler,
    DatasetFormat,
)

T = TypeVar("T")


class PandasToParquetEncodingHandlers(DatasetEncodingHandler):
    def encode(self, df: Optional[pd.DataFrame] = None, path: Optional[os.PathLike] = None) -> Optional[bytes]:
        return df.to_parquet(path, storage_options=_get_storage_config(path))


class ParquetToPandasDecodingHandler(DatasetDecodingHandler):
    def decode(self, df: Optional[Any] = None, path: Optional[os.PathLike] = None) -> pd.DataFrame:
        return pandas.read_parquet(path, storage_options=_get_storage_config(path))


class ArrowToParquetEncodingHandlers(DatasetEncodingHandler):
    def encode(self, df: Optional[Any] = None, path: Optional[os.PathLike] = None):
        pq.write_table(df, path, filesystem=_get_filesystem(path))


class ParquetToArrowDecodingHandler(DatasetDecodingHandler):
    def decode(self, df: Optional[Any] = None, path: Optional[os.PathLike] = None) -> pa.Table:
        return pq.read_table(path, filesystem=_get_filesystem(path))


def _get_filesystem(path: Union[str, os.PathLike]) -> Optional["fsspec.AbstractFileSystem"]:
    import fsspec

    from flytekit.core.data_persistence import split_protocol

    protocol, _ = split_protocol(path)
    if protocol == "s3":
        kwargs = _get_s3_config()
        return fsspec.filesystem(protocol, **kwargs)  # type: ignore
    return None


def _get_storage_config(path: Union[str, os.PathLike]) -> dict:
    from flytekit.core.data_persistence import split_protocol

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


FLYTE_DATASET_TRANSFORMER.register_handler(pd.DataFrame, DatasetFormat.PARQUET, PandasToParquetEncodingHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.PARQUET, pd.DataFrame, ParquetToPandasDecodingHandler())
FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.PARQUET, ArrowToParquetEncodingHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.PARQUET, pa.Table, ParquetToArrowDecodingHandler())

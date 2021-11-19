import os
import typing
from typing import TypeVar

import fsspec
import pandas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fsspec.core import split_protocol

from flytekit.configuration import aws as _aws_config
from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    DatasetFormat,
    DatasetPersistenceHandler,
    DatasetRetrievalHandler,
)

T = TypeVar("T")


class PandasToParquetPersistenceHandlers(DatasetPersistenceHandler):
    def persist(
        self,
        df: pandas.DataFrame,
        path: typing.Union[os.PathLike, str],
        coerce_timestamps: str = "us",
        allow_truncated_timestamps: bool = False,
    ):
        """
        Writes data frame as a chunk to the local directory owned by the Schema object.  Will later be uploaded to s3.
        :param df: data frame to write as parquet
        :param path: Sink file to write the dataframe to
        :param coerce_timestamps: format to store timestamp in parquet. 'us', 'ms', 's' are allowed values.
            Note: if your timestamps will lose data due to the coercion, your write will fail!  Nanoseconds are
            problematic in the Parquet format and will not work. See allow_truncated_timestamps.
        :param allow_truncated_timestamps: default False. Allow truncation when coercing timestamps to a coarser
            resolution.
        """
        df.to_parquet(
            path,
            coerce_timestamps=coerce_timestamps,
            allow_truncated_timestamps=allow_truncated_timestamps,
            storage_options=_get_storage_config(path),
        )


class ParquetToPandasRetrievalHandler(DatasetRetrievalHandler):
    def retrieve(self, path) -> pd.DataFrame:
        return pandas.read_parquet(path, storage_options=_get_storage_config(path))


class ArrowToParquetPersistenceHandlers(DatasetPersistenceHandler):
    def persist(
        self,
        table: pa.Table,
        path: os.PathLike,
    ):
        pq.write_table(table, path, filesystem=_get_filesystem(path))


class ParquetToArrowRetrievalHandler(DatasetRetrievalHandler):
    def retrieve(self, path: str) -> pa.Table:
        return pq.read_table(path, filesystem=_get_filesystem(path))


def _get_filesystem(path: typing.Union[str, os.PathLike]) -> typing.Optional[fsspec.AbstractFileSystem]:
    protocol, _ = split_protocol(path)
    if protocol == "s3":
        kwargs = _get_s3_config()
        return fsspec.filesystem(protocol, **kwargs)  # type: ignore
    return None


def _get_storage_config(path: typing.Union[str, os.PathLike]) -> dict:
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


FLYTE_DATASET_TRANSFORMER.register_handler(pd.DataFrame, DatasetFormat.PARQUET, PandasToParquetPersistenceHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.PARQUET, pd.DataFrame, ParquetToPandasRetrievalHandler())
FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.PARQUET, ArrowToParquetPersistenceHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.PARQUET, pa.Table, ParquetToArrowRetrievalHandler())

import os
import typing
from pathlib import Path
from typing import TypeVar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import NoCredentialsError
from fsspec.core import split_protocol, strip_protocol
from fsspec.utils import get_protocol

from flytekit import FlyteContext, logger
from flytekit.configuration import DataConfig
from flytekit.core.data_persistence import s3_setup_args
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    CSV,
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)

T = TypeVar("T")


def get_storage_options(cfg: DataConfig, uri: str, anon: bool = False) -> typing.Optional[typing.Dict]:
    protocol = get_protocol(uri)
    if protocol == "s3":
        kwargs = s3_setup_args(cfg.s3, anon)
        if kwargs:
            return kwargs
    return None


class PandasToCSVEncodingHandler(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pd.DataFrame, None, CSV)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        uri = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        if not ctx.file_access.is_remote(uri):
            Path(uri).mkdir(parents=True, exist_ok=True)
        path = os.path.join(uri, ".csv")
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        df.to_csv(
            path,
            index=False,
            storage_options=get_storage_options(ctx.file_access.data_config, path),
        )
        structured_dataset_type.format = CSV
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class CSVToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pd.DataFrame, None, CSV)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
        uri = flyte_value.uri
        columns = None
        kwargs = get_storage_options(ctx.file_access.data_config, uri)
        path = os.path.join(uri, ".csv")
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
        try:
            return pd.read_csv(path, usecols=columns, storage_options=kwargs)
        except NoCredentialsError:
            logger.debug("S3 source detected, attempting anonymous S3 access")
            kwargs = get_storage_options(ctx.file_access.data_config, uri, anon=True)
            return pd.read_csv(path, usecols=columns, storage_options=kwargs)


class PandasToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pd.DataFrame, None, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        uri = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        if not ctx.file_access.is_remote(uri):
            Path(uri).mkdir(parents=True, exist_ok=True)
        path = os.path.join(uri, f"{0:05}")
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        df.to_parquet(
            path,
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
            storage_options=get_storage_options(ctx.file_access.data_config, path),
        )
        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pd.DataFrame, None, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
        uri = flyte_value.uri
        columns = None
        kwargs = get_storage_options(ctx.file_access.data_config, uri)
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
        try:
            return pd.read_parquet(uri, columns=columns, storage_options=kwargs)
        except NoCredentialsError:
            logger.debug("S3 source detected, attempting anonymous S3 access")
            kwargs = get_storage_options(ctx.file_access.data_config, uri, anon=True)
            return pd.read_parquet(uri, columns=columns, storage_options=kwargs)


class ArrowToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pa.Table, None, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        uri = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        if not ctx.file_access.is_remote(uri):
            Path(uri).mkdir(parents=True, exist_ok=True)
        path = os.path.join(uri, f"{0:05}")
        filesystem = ctx.file_access.get_filesystem_for_path(path)
        pq.write_table(structured_dataset.dataframe, strip_protocol(path), filesystem=filesystem)
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pa.Table, None, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pa.Table:
        uri = flyte_value.uri
        if not ctx.file_access.is_remote(uri):
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
        _, path = split_protocol(uri)

        columns = None
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
        try:
            fs = ctx.file_access.get_filesystem_for_path(uri)
            return pq.read_table(path, filesystem=fs, columns=columns)
        except NoCredentialsError as e:
            logger.debug("S3 source detected, attempting anonymous S3 access")
            fs = ctx.file_access.get_filesystem_for_path(uri, anonymous=True)
            if fs is not None:
                return pq.read_table(path, filesystem=fs, columns=columns)
            raise e

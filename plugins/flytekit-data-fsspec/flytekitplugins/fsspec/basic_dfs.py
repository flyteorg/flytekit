import os
import typing
from pathlib import Path
from typing import TypeVar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from flytekitplugins.fsspec.persist import FSSpecPersistence, s3_setup_args
from fsspec.core import split_protocol, strip_protocol

from flytekit import FlyteContext
from flytekit.configuration import aws as _aws_config
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    LOCAL,
    PARQUET,
    S3,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)

T = TypeVar("T")


def get_storage_options(uri: str) -> typing.Optional[typing.Dict]:
    protocol = FSSpecPersistence._get_protocol(uri)
    if protocol == S3:
        s3_setup_args()
        if _aws_config.S3_ENDPOINT.get() is not None:
            # https://s3fs.readthedocs.io/en/latest/#s3-compatible-storage
            return {"client_kwargs": {"endpoint_url": _aws_config.S3_ENDPOINT.get()}}
    return None


class PandasToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)

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
            path, coerce_timestamps="us", allow_truncated_timestamps=False, storage_options=get_storage_options(path)
        )
        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pd.DataFrame:
        uri = flyte_value.uri
        if flyte_value.metadata.structured_dataset_type.columns:
            columns = []
            for c in flyte_value.metadata.structured_dataset_type.columns:
                columns.append(c.name)
            return pd.read_parquet(uri, columns=columns, storage_options=get_storage_options(uri))
        return pd.read_parquet(uri, storage_options=get_storage_options(uri))


class ArrowToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

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
        filesystem = FSSpecPersistence._get_filesystem(path)
        pq.write_table(structured_dataset.dataframe, strip_protocol(path), filesystem=filesystem)
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pa.Table:
        uri = flyte_value.uri
        if not ctx.file_access.is_remote(uri):
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
        _, path = split_protocol(uri)
        filesystem = FSSpecPersistence._get_filesystem(uri)
        if flyte_value.metadata.structured_dataset_type.columns:
            columns = []
            for c in flyte_value.metadata.structured_dataset_type.columns:
                columns.append(c.name)
            return pq.read_table(path, filesystem=filesystem, columns=columns)
        return pq.read_table(uri, filesystem=filesystem)


for protocol in [LOCAL, S3]:
    StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(protocol), True, True)
    StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(protocol), True, True)
    StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(protocol), True, True)
    StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(protocol), True, True)

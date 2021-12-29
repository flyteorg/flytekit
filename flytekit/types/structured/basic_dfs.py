import typing
from typing import TypeVar

import pandas
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from flytekit import FlyteContext
from flytekit.core.data_persistence import DataPersistencePlugins, split_protocol
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    LOCAL,
    PARQUET,
    S3,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)
from flytekit.types.structured.utils import get_filesystem, get_storage_config

T = TypeVar("T")


class PandasToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)
        self._persistence = DataPersistencePlugins.find_plugin(protocol)()  # want to use this somehow

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
    ) -> literals.StructuredDataset:

        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        local_dir = ctx.file_access.get_random_local_directory()
        local_path = os.path.join(local_dir, f"{0:05}")
        df.to_parquet(local_path, coerce_timestamps="us", allow_truncated_timestamps=False)
        ctx.file_access.upload_directory(local_dir, path)
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(format=PARQUET))


class ParquetToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pd.DataFrame:
        path = flyte_value.uri
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(path, local_dir, is_multipart=True)
        frames = [pandas.read_parquet(os.path.join(local_dir, f)) for f in os.listdir(local_dir)]
        if len(frames) == 1:
            return frames[0]
        elif len(frames) > 1:
            return pandas.concat(frames, copy=True)


class ArrowToParquetEncodingHandler(StructuredDatasetEncoder):
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
    ) -> literals.StructuredDataset:
        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_path()
        df = structured_dataset.dataframe
        pq.write_table(df, path, filesystem=get_filesystem(path))
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(format=PARQUET))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pa.Table:
        path = flyte_value.uri
        return pq.read_table(path, filesystem=get_filesystem(path))


class SparkToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(DataFrame, protocol, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
    ) -> literals.StructuredDataset:
        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_path()
        df = typing.cast(DataFrame, structured_dataset.dataframe)
        df.write.parquet(path)
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(format=PARQUET))


class ParquetToSparkDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(DataFrame, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        path = flyte_value.uri or ctx.file_access.get_random_remote_path()
        return spark.read.parquet(path)


for protocol in [LOCAL]:  # Think how to add S3 and GCS
    FLYTE_DATASET_TRANSFORMER.register_handler(PandasToParquetEncodingHandler(protocol), default_for_type=True)
    FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToPandasDecodingHandler(protocol), default_for_type=True)
    FLYTE_DATASET_TRANSFORMER.register_handler(ArrowToParquetEncodingHandler(pa.Table, protocol, PARQUET))
    FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToArrowDecodingHandler(pa.Table, protocol, PARQUET))
    FLYTE_DATASET_TRANSFORMER.register_handler(SparkToParquetEncodingHandler(protocol), default_for_type=True)
    FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToSparkDecodingHandler(protocol), default_for_type=True)

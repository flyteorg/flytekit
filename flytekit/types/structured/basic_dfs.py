import typing
from typing import TypeVar

import pandas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from flytekit import FlyteContext
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
from flytekit.core.data_persistence import split_protocol, DataPersistencePlugins

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

        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_path()
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        local_path = ctx.file_access.get_random_local_path()
        df.to_parquet(local_path)
        ctx.file_access.upload(local_path, path)

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
        local = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(path, local)
        return pandas.read_parquet(local)


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
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        path = flyte_value.uri or ctx.file_access.get_random_remote_path()
        return spark.read.parquet(path)


for protocol in [LOCAL]:  # Think how to add S3 and GCS
    FLYTE_DATASET_TRANSFORMER.register_handler(PandasToParquetEncodingHandler(protocol))
    FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToPandasDecodingHandler(protocol))
    FLYTE_DATASET_TRANSFORMER.register_handler(ArrowToParquetEncodingHandler(pa.Table, protocol, PARQUET))
    FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToArrowDecodingHandler(pa.Table, protocol, PARQUET))
    FLYTE_DATASET_TRANSFORMER.register_handler(SparkToParquetEncodingHandler(DataFrame, protocol, PARQUET))
    FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToSparkDecodingHandler(DataFrame, protocol, PARQUET))

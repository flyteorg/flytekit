import typing
from typing import TypeVar

import pandas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from flytekit import FlyteContext
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)
from flytekit.types.structured.utils import get_filesystem, get_storage_config

T = TypeVar("T")


class PandasToParquetEncodingHandlers(StructuredDatasetEncoder):
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
    ) -> literals.StructuredDataset:

        path = typing.cast(str, structured_dataset.uri)
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        df.to_parquet(path, storage_options=get_storage_config(path))

        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(format="parquet"))


class ParquetToPandasDecodingHandler(StructuredDatasetDecoder):
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pd.DataFrame:
        path = flyte_value.uri
        return pandas.read_parquet(path, storage_options=get_storage_config(path))


class ArrowToParquetEncodingHandlers(StructuredDatasetEncoder):
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
    ) -> literals.StructuredDataset:
        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_path()
        df = structured_dataset.dataframe
        pq.write_table(df, path, filesystem=get_filesystem(path))
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(format="parquet"))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pa.Table:
        path = flyte_value.uri
        return pq.read_table(path, filesystem=get_filesystem(path))


FLYTE_DATASET_TRANSFORMER.register_handler(PandasToParquetEncodingHandlers(pd.DataFrame, "local", "parquet"))
FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToPandasDecodingHandler(pd.DataFrame, "local", "parquet"))
FLYTE_DATASET_TRANSFORMER.register_handler(ArrowToParquetEncodingHandlers(pa.Table, "local", "parquet"))
FLYTE_DATASET_TRANSFORMER.register_handler(ParquetToArrowDecodingHandler(pa.Table, "local", "parquet"))

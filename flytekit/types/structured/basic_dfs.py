import os
import typing
from typing import TypeVar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from flytekit import FlyteContext
from flytekit.core.data_persistence import DataPersistencePlugins
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    GCS,
    LOCAL,
    PARQUET,
    S3,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)

T = TypeVar("T")


class PandasToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)
        # todo: Use this somehow instead of relaying ont he ctx file_access
        self._persistence = DataPersistencePlugins.find_plugin(protocol)()

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:

        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        local_dir = ctx.file_access.get_random_local_directory()
        local_path = os.path.join(local_dir, f"{0:05}")
        df.to_parquet(local_path, coerce_timestamps="us", allow_truncated_timestamps=False)
        ctx.file_access.upload_directory(local_dir, path)
        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
        path = flyte_value.uri
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(path, local_dir, is_multipart=True)
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return pd.read_parquet(local_dir, columns=columns)
        return pd.read_parquet(local_dir)


class ArrowToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        path = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_path()
        df = structured_dataset.dataframe
        local_dir = ctx.file_access.get_random_local_directory()
        local_path = os.path.join(local_dir, f"{0:05}")
        pq.write_table(df, local_path)
        ctx.file_access.upload_directory(local_dir, path)
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pa.Table:
        path = flyte_value.uri
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(path, local_dir, is_multipart=True)
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return pq.read_table(local_dir, columns=columns)
        return pq.read_table(local_dir)


for protocol in [LOCAL, S3]:
    StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(protocol))
    StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(protocol))
    StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(protocol))
    StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(protocol))

# Don't override the default for GCS.
StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(GCS), default_for_type=False)
StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(GCS), default_for_type=False)
StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(GCS), default_for_type=False)
StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(GCS), default_for_type=False)

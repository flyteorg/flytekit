import typing

import pandas as pd
import polars as pl

from flytekit import FlyteContext
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)


class PolarsDataFrameRenderer:
    """
    The Polars DataFrame summary statistics are rendered as an HTML table.
    """

    def to_html(self, df: pl.DataFrame) -> str:
        assert isinstance(df, pl.DataFrame)
        describe_df = df.describe()
        return pd.DataFrame(describe_df.transpose(), columns=describe_df.columns).to_html(index=False)


class PolarsDataFrameToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pl.DataFrame, None, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        df = typing.cast(pl.DataFrame, structured_dataset.dataframe)

        local_dir = ctx.file_access.get_random_local_directory()
        local_path = f"{local_dir}/00000"

        # Polars 0.13.12 deprecated to_parquet in favor of write_parquet
        if hasattr(df, "write_parquet"):
            df.write_parquet(local_path)
        else:
            df.to_parquet(local_path)
        remote_dir = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        ctx.file_access.upload_directory(local_dir, remote_dir)
        return literals.StructuredDataset(uri=remote_dir, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPolarsDataFrameDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pl.DataFrame, None, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pl.DataFrame:
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(flyte_value.uri, local_dir, is_multipart=True)
        path = f"{local_dir}/00000"
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return pl.read_parquet(path, columns=columns)
        return pl.read_parquet(path)


StructuredDatasetTransformerEngine.register(PolarsDataFrameToParquetEncodingHandler())
StructuredDatasetTransformerEngine.register(ParquetToPolarsDataFrameDecodingHandler())
StructuredDatasetTransformerEngine.register_renderer(pl.DataFrame, PolarsDataFrameRenderer())

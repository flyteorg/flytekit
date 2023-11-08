import typing

import pandas as pd
import polars as pl
from fsspec.utils import get_protocol

from flytekit import FlyteContext
from flytekit.core.data_persistence import get_fsspec_storage_options
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
        default_parquet_fn = "00000"
        local_dir = ctx.file_access.get_random_local_directory()
        local_path = ctx.file_access.join(
            local_dir,
            default_parquet_fn,
        )
        # Polars 0.13.12 deprecated to_parquet in favor of write_parquet
        if hasattr(df, "write_parquet"):
            df.write_parquet(local_path)
        else:
            df.to_parquet(local_path)

        if structured_dataset.uri is not None:
            remote_dir = structured_dataset.uri
            sd_uri = ctx.file_access.join(
                remote_dir,
                default_parquet_fn,
            )
        else:
            remote_dir = ctx.file_access.get_random_remote_directory()
            sd_uri = ctx.file_access.join(
                remote_dir,
                default_parquet_fn,
            )
        ctx.file_access.upload_directory(local_dir, remote_dir)
        return literals.StructuredDataset(uri=sd_uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPolarsDataFrameDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pl.DataFrame, None, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pl.DataFrame:
        uri = flyte_value.uri

        kwargs = get_fsspec_storage_options(protocol=get_protocol(uri), data_config=ctx.file_access.data_config)
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return pl.read_parquet(uri, columns=columns, use_pyarrow=True, storage_options=kwargs)
        return pl.read_parquet(uri, use_pyarrow=True, storage_options=kwargs)


StructuredDatasetTransformerEngine.register(PolarsDataFrameToParquetEncodingHandler())
StructuredDatasetTransformerEngine.register(ParquetToPolarsDataFrameDecodingHandler())
StructuredDatasetTransformerEngine.register_renderer(pl.DataFrame, PolarsDataFrameRenderer())

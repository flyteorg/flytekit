import io
import typing

from flytekit import FlyteContext, lazy_module
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

if typing.TYPE_CHECKING:
    import fsspec.utils as fsspec_utils

    import polars as pl
else:
    pl = lazy_module("polars")
    fsspec_utils = lazy_module("fsspec.utils")


############################
# Polars DataFrame classes #
############################


class PolarsDataFrameRenderer:
    """
    The Polars DataFrame summary statistics are rendered as an HTML table.
    """

    def to_html(self, df: typing.Union[pl.DataFrame, pl.LazyFrame]) -> str:
        assert isinstance(df, (pl.DataFrame, pl.LazyFrame))
        try:
            describe_df = df.describe()
        except AttributeError:
            # LazyFrames in polars <= 0.19 does not support `describe`
            describe_df = df.collect().describe()

        # the value is "statistic" or "describe" depending on polars version
        stat_colname = describe_df.columns[0]

        columns = describe_df[stat_colname]
        html_repr = describe_df.drop(stat_colname).transpose(column_names=columns)._repr_html_()
        return html_repr


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

        output_bytes = io.BytesIO()
        # Polars 0.13.12 deprecated to_parquet in favor of write_parquet
        if hasattr(df, "write_parquet"):
            df.write_parquet(output_bytes)
        else:
            df.to_parquet(output_bytes)

        if structured_dataset.uri is not None:
            fs = ctx.file_access.get_filesystem_for_path(path=structured_dataset.uri)
            with fs.open(structured_dataset.uri, "wb") as s:
                s.write(output_bytes)
            output_uri = structured_dataset.uri
        else:
            remote_fn = "00000"  # 00000 is our default unnamed parquet filename
            output_uri = ctx.file_access.put_raw_data(output_bytes, file_name=remote_fn)
        return literals.StructuredDataset(uri=output_uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


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

        kwargs = get_fsspec_storage_options(
            protocol=fsspec_utils.get_protocol(uri),
            data_config=ctx.file_access.data_config,
        )
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return pl.read_parquet(uri, columns=columns, use_pyarrow=True, storage_options=kwargs)
        return pl.read_parquet(uri, use_pyarrow=True, storage_options=kwargs)


############################
# Polars LazyFrame classes #
############################


class PolarsLazyFrameToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pl.LazyFrame, None, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        lf = typing.cast(pl.LazyFrame, structured_dataset.dataframe)

        # The pl.LazyFrame.sink_parquet method uses streaming mode, which is currently considered unstable. Until it is
        # stable, we collect the dataframe and write it to a BytesIO buffer.
        df = lf.collect()

        if hasattr(df, "write_parquet"):
            # Polars 0.13.12 deprecated to_parquet in favor of write_parquet
            _write_method = df.write_parquet
        else:
            _write_method = df.to_parquet

        if structured_dataset.uri is not None:
            fs = ctx.file_access.get_filesystem_for_path(path=structured_dataset.uri)
            with fs.open(structured_dataset.uri, "wb") as s:
                _write_method(s)
            output_uri = structured_dataset.uri
        else:
            output_bytes = io.BytesIO()
            remote_fn = "00000"  # 00000 is our default unnamed parquet filename
            _write_method(output_bytes)
            output_uri = ctx.file_access.put_raw_data(output_bytes, file_name=remote_fn)
        return literals.StructuredDataset(uri=output_uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPolarsLazyFrameDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pl.LazyFrame, None, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pl.LazyFrame:
        uri = flyte_value.uri

        kwargs = get_fsspec_storage_options(
            protocol=fsspec_utils.get_protocol(uri),
            data_config=ctx.file_access.data_config,
        )
        # use read_parquet instead of scan_parquet for now because scan_parquet currently doesn't work with fsspec:
        # https://github.com/pola-rs/polars/issues/16737
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return pl.read_parquet(uri, columns=columns, use_pyarrow=True, storage_options=kwargs).lazy()
        return pl.read_parquet(uri, use_pyarrow=True, storage_options=kwargs).lazy()


# Register the Polars DataFrame handlers
StructuredDatasetTransformerEngine.register(PolarsDataFrameToParquetEncodingHandler())
StructuredDatasetTransformerEngine.register(ParquetToPolarsDataFrameDecodingHandler())
StructuredDatasetTransformerEngine.register_renderer(pl.DataFrame, PolarsDataFrameRenderer())

# Register the Polars LazyFrame handlers
StructuredDatasetTransformerEngine.register(PolarsLazyFrameToParquetEncodingHandler())
StructuredDatasetTransformerEngine.register(ParquetToPolarsLazyFrameDecodingHandler())

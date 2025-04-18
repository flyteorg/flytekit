import typing
from pathlib import Path

from flytekit import FlyteContext, lazy_module
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
    import pyarrow

    import geopandas as gpd
else:
    gpd = lazy_module("geopandas")
    pyarrow = lazy_module("pyarrow")


class GeoPandasDataFrameRenderer:
    """
    The Geopandas DataFrame summary statistics are rendered as an HTML table.
    """

    def to_html(self, df: gpd.GeoDataFrame) -> str:
        assert isinstance(df, gpd.GeoDataFrame)
        return df.describe()._repr_html_()


class GeoPandasEncodingHandler(StructuredDatasetEncoder):
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        uri = typing.cast(str, structured_dataset.uri) or ctx.file_access.join(
            ctx.file_access.raw_output_prefix, ctx.file_access.get_random_string()
        )
        if not ctx.file_access.is_remote(uri):
            Path(uri).mkdir(parents=True, exist_ok=True)
        uri = str(Path(uri) / "data.parquet")
        df = typing.cast(gpd.GeoDataFrame, structured_dataset.dataframe)
        df.to_parquet(uri)
        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class GeoPandasDecodingHandler(StructuredDatasetDecoder):
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> gpd.GeoDataFrame:
        # a user may want to bring a non-parquet gdf, which uses a different
        # opening method.
        try:
            return gpd.read_parquet(flyte_value.uri)
        except pyarrow.lib.ArrowInvalid:
            return gpd.read_file(flyte_value.uri)


StructuredDatasetTransformerEngine.register_renderer(gpd.GeoDataFrame, GeoPandasDataFrameRenderer())
# We register GeoPandas encoder to support parquet between and from tasks / workflows
StructuredDatasetTransformerEngine.register(GeoPandasEncodingHandler(gpd.GeoDataFrame, None, PARQUET))
# We register to any format for decoder in the event a user provides geopackage,
# shape file, parquet, etc.
StructuredDatasetTransformerEngine.register(GeoPandasDecodingHandler(gpd.GeoDataFrame, None, None))

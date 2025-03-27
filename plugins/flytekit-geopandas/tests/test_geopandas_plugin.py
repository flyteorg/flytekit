import geopandas as gpd
from flytekitplugins.geopandas.gdf_transformers import GeoPandasDataFrameRenderer
from pathlib import Path

import pytest

from flytekit import task
from flytekit.types.structured.structured_dataset import StructuredDataset
import numpy as np


def test_geopandas_encodes_decodes():
    @task
    def _gdf_task(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gdf

    gdf = gpd.GeoDataFrame(
        {"geometry": gpd.points_from_xy([0, 1], [0, 1]), "other_column": [1, 2]},
        crs="EPSG:4326",
    )
    rt_gdf = _gdf_task(gdf)
    assert rt_gdf.equals(gdf)


@pytest.mark.parametrize("file_name", ["output.geojson", "output.gpkg"])
def test_geopandas_encodes_common_formats(tmp_path: Path, file_name: str):
    @task
    def _gdf_task(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gdf

    gdf = gpd.GeoDataFrame(
        {"other": np.array([1.0, 2.0])},
        geometry=gpd.points_from_xy([0, 1], [0, 1]),
        crs="EPSG:4326",
    )
    uri = str(tmp_path / file_name)
    gdf.to_file(uri)
    rt_gdf = _gdf_task(gdf=StructuredDataset(uri=uri))
    assert rt_gdf.equals(gdf)


def test_geopandas_encodes_shp_not_yet_supported(tmp_path: Path):
    @task
    def _gdf_task(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gdf

    gdf = gpd.GeoDataFrame(
        {"geometry": gpd.points_from_xy([0, 1], [0, 1]), "other": [1, 2]},
        crs="EPSG:4326",
    )
    uri = str(tmp_path / "output.shp")
    gdf.to_file(uri)
    with pytest.raises(ValueError, match=r"Set SHAPE_RESTORE_SHX config option to YES"):
        rt_gdf = _gdf_task(gdf=StructuredDataset(uri=uri))


def test_gdf_renderer():
    gdf = gpd.GeoDataFrame(
        {"geometry": gpd.points_from_xy([0, 1], [0, 1]), "other_column": [1, 2]},
        crs="EPSG:4326",
    )
    described = gdf.describe()._repr_html_()
    assert GeoPandasDataFrameRenderer().to_html(gdf) == described

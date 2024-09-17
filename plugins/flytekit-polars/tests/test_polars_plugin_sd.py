import tempfile

import pandas as pd
import polars as pl
import pytest
from flytekitplugins.polars.sd_transformers import PolarsDataFrameRenderer
from typing_extensions import Annotated
from packaging import version
from polars.testing import assert_frame_equal

from flytekit import kwtypes, task, workflow
from flytekit.types.structured.structured_dataset import PARQUET, StructuredDataset

subset_schema = Annotated[StructuredDataset, kwtypes(col2=str), PARQUET]
full_schema = Annotated[StructuredDataset, PARQUET]

polars_version = pl.__version__


@pytest.mark.parametrize("df_cls", [pl.DataFrame, pl.LazyFrame])
def test_polars_workflow_subset(df_cls):
    @task
    def generate() -> subset_schema:
        df = df_cls({"col1": [1, 3, 2], "col2": list("abc")})
        return StructuredDataset(dataframe=df)

    @task
    def consume(df: subset_schema) -> subset_schema:
        df = df.open(df_cls).all()

        materialized_df = df
        if df_cls is pl.LazyFrame:
            materialized_df = df.collect()

        assert materialized_df["col2"][0] == "a"
        assert materialized_df["col2"][1] == "b"
        assert materialized_df["col2"][2] == "c"

        return StructuredDataset(dataframe=df)

    @workflow
    def wf() -> subset_schema:
        return consume(df=generate())

    result = wf()
    assert result is not None


@pytest.mark.parametrize("df_cls", [pl.DataFrame, pl.LazyFrame])
def test_polars_workflow_full(df_cls):
    @task
    def generate() -> full_schema:
        df = df_cls({"col1": [1, 3, 2], "col2": list("abc")})
        return StructuredDataset(dataframe=df)

    @task
    def consume(df: full_schema) -> full_schema:
        df = df.open(df_cls).all()

        materialized_df = df
        if df_cls is pl.LazyFrame:
            materialized_df = df.collect()

        assert materialized_df["col1"][0] == 1
        assert materialized_df["col1"][1] == 3
        assert materialized_df["col1"][2] == 2
        assert materialized_df["col2"][0] == "a"
        assert materialized_df["col2"][1] == "b"
        assert materialized_df["col2"][2] == "c"

        return StructuredDataset(dataframe=df.sort("col1"))

    @workflow
    def wf() -> full_schema:
        return consume(df=generate())

    result = wf()
    assert result is not None


@pytest.mark.parametrize("df_cls", [pl.DataFrame, pl.LazyFrame])
def test_polars_renderer(df_cls):
    df = df_cls({"col1": [1, 3, 2], "col2": list("abc")})

    if df_cls is pl.LazyFrame:
        df_desc = df.collect().describe()
    else:
        df_desc = df.describe()

    stat_colname = df_desc.columns[0]
    expected = df_desc.drop(stat_colname).transpose(column_names=df_desc[stat_colname])._repr_html_()
    assert PolarsDataFrameRenderer().to_html(df) == expected


@pytest.mark.parametrize("df_cls", [pl.DataFrame, pl.LazyFrame])
def test_parquet_to_polars_dataframe(df_cls):
    data = {"name": ["Alice"], "age": [5]}

    @task
    def create_sd() -> StructuredDataset:
        df = df_cls(data=data)
        return StructuredDataset(dataframe=df)

    sd = create_sd()
    polars_df = sd.open(df_cls).all()
    if isinstance(polars_df, pl.LazyFrame):
        polars_df = polars_df.collect()

    assert_frame_equal(pl.DataFrame(data), polars_df)

    tmp = tempfile.mktemp()
    pl.DataFrame(data).write_parquet(tmp)

    @task
    def consume_sd_return_df(sd: StructuredDataset) -> df_cls:
        return sd.open(df_cls).all()

    sd = StructuredDataset(uri=tmp)
    df_out = consume_sd_return_df(sd=sd)

    if df_cls is pl.LazyFrame:
        df_out = df_out.collect()

    assert_frame_equal(df_out, polars_df)

    @task
    def consume_sd_return_sd(sd: StructuredDataset) -> StructuredDataset:
        return StructuredDataset(dataframe=sd.open(df_cls).all())

    sd = StructuredDataset(uri=tmp)
    opened_sd = consume_sd_return_sd(sd=sd).open(df_cls).all()

    if df_cls is pl.LazyFrame:
        opened_sd = opened_sd.collect()

    assert_frame_equal(opened_sd, polars_df)

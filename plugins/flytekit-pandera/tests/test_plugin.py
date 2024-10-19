import typing

import pandas
import pandera
import pytest
from pandera.typing.pandas import DataFrame, Series

from flytekit import current_context, task, workflow
from flytekitplugins.pandera import ValidationConfig, PandasReportRenderer


def test_pandera_dataframe_type_hints():
    class InSchema(pandera.DataFrameModel):
        col1: int
        col2: float

    class IntermediateSchema(InSchema):
        col3: float

        @pandera.dataframe_check
        @classmethod
        def col3_check(cls, df: DataFrame) -> Series[bool]:
            return df["col3"] == df["col1"] * df["col2"]

    class OutSchema(IntermediateSchema):
        col4: str

    @task
    def transform1(df: DataFrame[InSchema]) -> DataFrame[IntermediateSchema]:
        return df.assign(col3=df["col1"] * df["col2"])

    @task
    def transform2(df: DataFrame[IntermediateSchema]) -> DataFrame[OutSchema]:
        return df.assign(col4="foo")

    valid_df = pandas.DataFrame({"col1": [1, 2, 3], "col2": [10.0, 11.0, 12.0]})

    @workflow
    def my_wf() -> DataFrame[OutSchema]:
        return transform2(df=transform1(df=valid_df))

    result = my_wf()
    assert isinstance(result, pandas.DataFrame)

    # raise error when defining workflow using invalid data
    invalid_df = pandas.DataFrame({"col1": [1, 2, 3], "col2": list("abc")})

    with pytest.raises(pandera.errors.SchemaErrors):

        @workflow
        def invalid_wf() -> DataFrame[OutSchema]:
            return transform2(df=transform1(df=invalid_df))

        invalid_wf()

    # raise error when executing workflow with invalid input
    @workflow
    def wf_with_df_input(df: DataFrame[InSchema]) -> DataFrame[OutSchema]:
        return transform2(df=transform1(df=df))

    with pytest.raises(pandera.errors.SchemaErrors):
        wf_with_df_input(df=invalid_df)

    # raise error when executing workflow with invalid output
    @task
    def transform2_noop(df: DataFrame[IntermediateSchema]) -> DataFrame[OutSchema]:
        return df

    @workflow
    def wf_invalid_output(df: DataFrame[InSchema]) -> DataFrame[OutSchema]:
        return transform2_noop(df=transform1(df=df))

    with pytest.raises(pandera.errors.SchemaErrors):
        wf_invalid_output(df=valid_df)


@pytest.mark.parametrize(
    "data",
    [
        pandas.DataFrame({"col1": [1, 2, 3]}),
        pandas.DataFrame({"col1": [1, 2, 3], "col2": list("abc")}),
        pandas.DataFrame(),
    ],
)
def test_pandera_dataframe_no_schema_model(data):
    @task
    def transform(df: DataFrame) -> DataFrame:
        return df

    @workflow
    def my_wf(df: DataFrame) -> DataFrame:
        return transform(df=df)

    result = my_wf(df=data)
    assert isinstance(result, pandas.DataFrame)


@pytest.mark.parametrize(
    "config",
    [
        ValidationConfig(on_error="warn"),
        ValidationConfig(on_error="raise"),
    ],
)
def test_pandera_dataframe_warn_on_error(config, capsys):
    class Schema(pandera.DataFrameModel):
        col1: int
        col2: float

    @task
    def fn_input(df: typing.Annotated[DataFrame[Schema], config]):
        return df

    @task
    def fn_output(df: DataFrame) -> typing.Annotated[DataFrame[Schema], config]:
        return df

    data = pandas.DataFrame()

    for fn in [fn_input, fn_output]:
        if config.on_error == "raise":
            with pytest.raises(pandera.errors.SchemaErrors):
                fn(df=data)
        elif config.on_error == "warn":
            fn(df=data)
            assert "WARNING" in capsys.readouterr().out
        else:
            raise ValueError(f"Invalid on_error value: {config.on_error}")


def test_pandera_report_renderer():
    class InSchema(pandera.DataFrameModel):
        col1: int
        col2: float

    class OutSchema(InSchema):
        col3: str

    config = ValidationConfig(on_error="warn")

    @task(enable_deck=True, deck_fields=[])
    def fn(df: typing.Annotated[DataFrame[InSchema], config]) -> typing.Annotated[DataFrame[OutSchema], config]:
        return df.assign(col_3="foo")

    fn(df=pandas.DataFrame())
    ctx = current_context()
    assert len(ctx.decks) == 2
    assert ctx.decks[0].name == "Pandera Report: InSchema"
    assert ctx.decks[1].name == "Pandera Report: OutSchema"

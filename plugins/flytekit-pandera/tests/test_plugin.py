import pandas
import pandera
import pytest

from flytekit import task, workflow


def test_pandera_dataframe_type_hints():
    class InSchema(pandera.DataFrameModel):
        col1: int
        col2: float

    class IntermediateSchema(InSchema):
        col3: float

        @pandera.dataframe_check
        @classmethod
        def col3_check(cls, df: pandera.typing.DataFrame) -> pandera.typing.Series[bool]:
            return df["col3"] == df["col1"] * df["col2"]

    class OutSchema(IntermediateSchema):
        col4: str

    @task
    def transform1(df: pandera.typing.DataFrame[InSchema]) -> pandera.typing.DataFrame[IntermediateSchema]:
        return df.assign(col3=df["col1"] * df["col2"])

    @task
    def transform2(df: pandera.typing.DataFrame[IntermediateSchema]) -> pandera.typing.DataFrame[OutSchema]:
        return df.assign(col4="foo")

    valid_df = pandas.DataFrame({"col1": [1, 2, 3], "col2": [10.0, 11.0, 12.0]})

    @workflow
    def my_wf() -> pandera.typing.DataFrame[OutSchema]:
        return transform2(df=transform1(df=valid_df))

    result = my_wf()
    assert isinstance(result, pandas.DataFrame)

    # raise error when defining workflow using invalid data
    invalid_df = pandas.DataFrame({"col1": [1, 2, 3], "col2": list("abc")})

    with pytest.raises(pandera.errors.SchemaErrors):

        @workflow
        def invalid_wf() -> pandera.typing.DataFrame[OutSchema]:
            return transform2(df=transform1(df=invalid_df))

        invalid_wf()

    # raise error when executing workflow with invalid input
    @workflow
    def wf_with_df_input(df: pandera.typing.DataFrame[InSchema]) -> pandera.typing.DataFrame[OutSchema]:
        return transform2(df=transform1(df=df))

    with pytest.raises(pandera.errors.SchemaErrors):
        wf_with_df_input(df=invalid_df)

    # raise error when executing workflow with invalid output
    @task
    def transform2_noop(df: pandera.typing.DataFrame[IntermediateSchema]) -> pandera.typing.DataFrame[OutSchema]:
        return df

    @workflow
    def wf_invalid_output(df: pandera.typing.DataFrame[InSchema]) -> pandera.typing.DataFrame[OutSchema]:
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
    def transform(df: pandera.typing.DataFrame) -> pandera.typing.DataFrame:
        return df

    @workflow
    def my_wf(df: pandera.typing.DataFrame) -> pandera.typing.DataFrame:
        return transform(df=df)

    result = my_wf(df=data)
    assert isinstance(result, pandas.DataFrame)

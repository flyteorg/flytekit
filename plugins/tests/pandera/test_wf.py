import pandas
import pandera
import pytest

from flytekit import task, workflow


def test_pandera_dataframe_type_hints():
    class InSchema(pandera.SchemaModel):
        col1: pandera.typing.Series[int]
        col2: pandera.typing.Series[float]

    class IntermediateSchema(InSchema):
        col3: pandera.typing.Series[float]

        @pandera.dataframe_check
        @classmethod
        def col3_check(cls, df: pandera.typing.DataFrame) -> pandera.typing.Series[bool]:
            return df["col3"] == df["col1"] * df["col2"]

    class OutSchema(IntermediateSchema):
        col4: pandera.typing.Series[str]

    @task
    def transform1(df: pandera.typing.DataFrame[InSchema]) -> pandera.typing.DataFrame[IntermediateSchema]:
        return df.assign(col3=df["col1"] * df["col2"])

    @task
    def transform2(df: pandera.typing.DataFrame[IntermediateSchema]) -> pandera.typing.DataFrame[OutSchema]:
        return df.assign(col4="foo")

    @workflow
    def my_wf() -> pandera.typing.DataFrame[OutSchema]:
        df = pandas.DataFrame({"col1": [1, 2, 3], "col2": [10.0, 11.0, 12.0]})
        return transform2(df=transform1(df=df))

    @workflow
    def invalid_wf() -> pandera.typing.DataFrame[OutSchema]:
        df = pandas.DataFrame({"col1": [1, 2, 3], "col2": list("abc")})
        return transform2(df=transform1(df=df))

    result = my_wf()
    assert isinstance(result, pandas.DataFrame)

    # raise error at runtime on invalid types
    with pytest.raises(pandera.errors.SchemaError):
        invalid_wf()


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

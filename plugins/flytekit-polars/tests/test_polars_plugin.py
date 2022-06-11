import polars as pl
from flytekitplugins.polars import schema  # noqa F401

from flytekit import task, workflow


def test_polars_workflow():
    @task
    def generate() -> pl.DataFrame:
        df = pl.DataFrame({"col1": [1, 3, 2], "col2": list("abc")})
        return df

    @task
    def consume(df: pl.DataFrame) -> pl.DataFrame:
        return df.sort("col1")

    @workflow
    def wf() -> pl.DataFrame:
        return consume(df=generate())

    result = wf()
    assert result is not None
    assert isinstance(result, pl.DataFrame)
    assert result["col1"][0] == 1
    assert result["col1"][1] == 2
    assert result["col1"][2] == 3
    assert result["col2"][0] == "a"
    assert result["col2"][1] == "c"
    assert result["col2"][2] == "b"

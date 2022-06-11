from flytekitplugins.polars import schema  # noqa F401
import polars as pl

from flytekit import task, workflow

def test_polars_workflow():
    @task
    def generate() -> pl.DataFrame:
        df = pl.DataFrame({"col1": [1, 2, 3], "col2": list("abc")})
        return df

    @task
    def consume(df: pl.DataFrame) -> pl.DataFrame:
        return df

    @workflow
    def wf() -> pl.DataFrame:
        return consume(df=generate())

    result = wf()
    assert result is not None
    assert isinstance(result, pl.DataFrame)

import vaex
from typing_extensions import Annotated

from flytekit import kwtypes, task, workflow
from flytekit.types.structured.structured_dataset import PARQUET, StructuredDataset

subset_schema = Annotated[StructuredDataset, kwtypes(col2=str), PARQUET]
full_schema = Annotated[StructuredDataset, PARQUET]
vaex_df = vaex.from_dict(dict(x=[1, 3, 2], y=["a", "b", "c"]))


def test_vaex_workflow_subset():
    @task
    def generate() -> subset_schema:
        return StructuredDataset(dataframe=vaex_df)

    @task
    def consume(df: subset_schema) -> subset_schema:
        df = df.open(vaex.dataframe.DataFrameLocal).all()
        col2 = df.col2.values.tolist()
        assert col2[0] == "a"
        assert col2[1] == "b"
        assert col2[2] == "c"
        return StructuredDataset(dataframe=df)

    @workflow
    def wf() -> subset_schema:
        return consume(df=generate())

    result = wf()
    assert result is not None


def test_vaex_workflow_full():
    @task
    def generate() -> full_schema:
        return StructuredDataset(dataframe=vaex_df)

    @task
    def consume(df: full_schema) -> full_schema:
        df = df.open(vaex.dataframe.DataFrameLocal).all()
        colx = df.x.values.tolist()
        coly = df.y.values.tolist()

        assert colx[0] == 1
        assert colx[1] == 3
        assert colx[2] == 2
        assert coly[0] == "a"
        assert coly[1] == "b"
        assert coly[2] == "c"

        return StructuredDataset(dataframe=df.sort("x"))

    @workflow
    def wf() -> full_schema:
        return consume(df=generate())

    result = wf()
    assert result is not None

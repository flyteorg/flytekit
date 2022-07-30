import datasets
import flytekitplugins.huggingface  # noqa F401
import pandas as pd

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

from flytekit import kwtypes, task, workflow
from flytekit.types.structured.structured_dataset import PARQUET, StructuredDataset

subset_schema = Annotated[StructuredDataset, kwtypes(col2=str), PARQUET]
full_schema = Annotated[StructuredDataset, PARQUET]


def test_huggingface_dataset_workflow_subset():
    @task
    def generate() -> subset_schema:
        df = pd.DataFrame({"col1": [1, 3, 2], "col2": list("abc")})
        return StructuredDataset(dataframe=df)

    @task
    def consume(df: subset_schema) -> subset_schema:
        dataset = df.open(datasets.Dataset).all()

        assert dataset[0]["col2"] == "a"
        assert dataset[1]["col2"] == "b"
        assert dataset[2]["col2"] == "c"

        return StructuredDataset(dataframe=dataset)

    @workflow
    def wf() -> subset_schema:
        return consume(df=generate())

    result = wf()
    assert result is not None


def test_huggingface_dataset__workflow_full():
    @task
    def generate() -> full_schema:
        df = pd.DataFrame({"col1": [1, 3, 2], "col2": list("abc")})
        return StructuredDataset(dataframe=df)

    @task
    def consume(df: full_schema) -> full_schema:
        dataset = df.open(datasets.Dataset).all()

        assert dataset[0]["col1"] == 1
        assert dataset[1]["col1"] == 3
        assert dataset[2]["col1"] == 2
        assert dataset[0]["col2"] == "a"
        assert dataset[1]["col2"] == "b"
        assert dataset[2]["col2"] == "c"

        return StructuredDataset(dataframe=dataset)

    @workflow
    def wf() -> full_schema:
        return consume(df=generate())

    result = wf()
    assert result is not None

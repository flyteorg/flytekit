from typing import Annotated

import datasets
import pandas as pd
from flytekitplugins.huggingface.sd_transformers import HuggingFaceDatasetRenderer

from flytekit import kwtypes, task, workflow
from flytekit.types.structured.structured_dataset import PARQUET, StructuredDataset

subset_schema = Annotated[StructuredDataset, kwtypes(col2=str), PARQUET]
full_schema = Annotated[StructuredDataset, PARQUET]


def test_huggingface_dataset_workflow_subset():
    @task
    def generate() -> subset_schema:
        df = pd.DataFrame({"col1": [1, 3, 2], "col2": list("abc")})
        dataset = datasets.Dataset.from_pandas(df)
        return StructuredDataset(dataframe=dataset)

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
        dataset = datasets.Dataset.from_pandas(df)
        return StructuredDataset(dataframe=dataset)

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


def test_datasets_renderer():
    df = pd.DataFrame({"col1": [1, 3, 2], "col2": list("abc")})
    dataset = datasets.Dataset.from_pandas(df)
    assert HuggingFaceDatasetRenderer().to_html(dataset) == str(dataset).replace("\n", "<br>")

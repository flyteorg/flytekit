import os

import pandas as pd
import pytest
from flytekitplugins.greatexpectations import BatchRequestConfig, GETask
from great_expectations import checkpoint
from great_expectations.exceptions import ValidationError

from flytekit import kwtypes, task, workflow


def test_ge_simple_task():
    task_object = GETask(
        name="test1",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    # valid data
    task_object.execute(dataset="yellow_tripdata_sample_2019-01.csv")

    # invalid data
    with pytest.raises(ValidationError):
        task_object.execute(dataset="yellow_tripdata_sample_2019-02.csv")

    assert task_object.python_interface.inputs == {"dataset": str}


def test_ge_batchrequest_pandas_config():
    task_object = GETask(
        name="test2",
        data_source="data",
        inputs=kwtypes(data=str),
        expectation_suite="test.demo",
        data_connector="my_data_connector",
        task_config=BatchRequestConfig(
            data_connector_query={
                "batch_filter_parameters": {
                    "year": "2019",
                    "month": "01",
                },
                "limit": 10,
            }
        ),
    )

    task_object.execute(data="my_assets")


def test_invalid_ge_batchrequest_pandas_config():
    task_object = GETask(
        name="test2",
        data_source="data",
        inputs=kwtypes(data=str),
        expectation_suite="test.demo",
        data_connector="my_data_connector",
        task_config=BatchRequestConfig(
            data_connector_query={
                "batch_filter_parameters": {
                    "year": "2020",
                },
            }
        ),
    )

    # Capture "ValueError: Got 0 batches instead of a single batch."
    with pytest.raises(ValueError, match=r".*0 batches.*"):
        task_object.execute(data="my_assets")


def test_ge_runtimebatchrequest_sqlite_config():
    task_object = GETask(
        name="test3",
        data_source="sqlite_data",
        inputs=kwtypes(dataset=str),
        expectation_suite="sqlite.movies",
        data_connector="sqlite_data_connector",
        task_config=BatchRequestConfig(
            runtime_parameters={"query": "SELECT * FROM movies"},
            batch_identifiers={
                "pipeline_stage": "validation",
            },
        ),
    )

    task_object.execute(dataset="movies.sqlite")


def test_ge_validation_within_task():
    task_object = GETask(
        name="test4",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def my_task(csv_file: str) -> int:
        task_object(dataset=str(csv_file))
        df = pd.read_csv(os.path.join("data", csv_file))
        return df.shape[0]

    @workflow
    def valid_wf(dataset: str = "yellow_tripdata_sample_2019-01.csv") -> int:
        return my_task(csv_file=dataset)

    @pytest.mark.xfail(strict=True)
    @workflow
    def invalid_wf(dataset: str = "yellow_tripdata_sample_2019-02.csv") -> int:
        return my_task(csv_file=dataset)

    valid_result = valid_wf()
    assert valid_result == 10000

    invalid_wf()


def test_ge_checkpoint_params():
    task_object = GETask(
        name="test5",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
        checkpoint_params={
            "site_names": ["local_site"],
        },
    )

    task_object.execute(dataset="yellow_tripdata_sample_2019-01.csv")

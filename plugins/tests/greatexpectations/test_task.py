import os

import pandas as pd
import pytest
from flytekitplugins.greatexpectations import BatchRequestConfig, GETask
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
    result = task_object.execute(dataset="yellow_tripdata_sample_2019-01.csv")

    assert result["success"] is True
    assert result["statistics"]["evaluated_expectations"] == result["statistics"]["successful_expectations"]

    # invalid data
    with pytest.raises(ValidationError):
        invalid_result = task_object.execute(dataset="yellow_tripdata_sample_2019-02.csv")
        assert invalid_result["success"] is False
        assert (
            invalid_result["statistics"]["evaluated_expectations"]
            != invalid_result["statistics"]["successful_expectations"]
        )

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
            },
            limit=10,
        ),
    )

    # name of the asset -- can be found in great_expectations.yml file
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


def test_ge_task():
    task_object = GETask(
        name="test4",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def my_task(csv_file: str) -> int:
        task_object(dataset=csv_file)
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


def test_ge_task_multiple_args():
    task_object_one = GETask(
        name="test5",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )
    task_object_two = GETask(
        name="test6",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test1.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def get_file_name(dataset_one: str, dataset_two: str) -> (int, int):
        task_object_one(dataset=dataset_one)
        task_object_two(dataset=dataset_two)
        df_one = pd.read_csv(os.path.join("data", dataset_one))
        df_two = pd.read_csv(os.path.join("data", dataset_two))
        return len(df_one), len(df_two)

    @workflow
    def wf(
        dataset_one: str = "yellow_tripdata_sample_2019-01.csv", dataset_two: str = "yellow_tripdata_sample_2019-02.csv"
    ) -> (int, int):
        return get_file_name(dataset_one=dataset_one, dataset_two=dataset_two)

    assert wf() == (10000, 10000)


def test_ge_workflow():
    task_object = GETask(
        name="test7",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    @workflow
    def valid_wf(dataset: str = "yellow_tripdata_sample_2019-01.csv") -> None:
        task_object(dataset=dataset)

    valid_wf()


def test_ge_checkpoint_params():
    task_object = GETask(
        name="test8",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
        checkpoint_params={
            "site_names": ["local_site"],
        },
    )

    task_object.execute(dataset="yellow_tripdata_sample_2019-01.csv")

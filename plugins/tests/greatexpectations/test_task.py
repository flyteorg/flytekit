import os
import sqlite3
import typing

import pandas as pd
import pytest
from flytekitplugins.greatexpectations import BatchRequestConfig, GETask
from great_expectations.exceptions import ValidationError

from flytekit import kwtypes, task, workflow
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema

if "tests/greatexpectations" not in os.getcwd():
    os.chdir("plugins/tests/greatexpectations/")


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
        name="test3",
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
        name="test4",
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
        name="test5",
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


def test_ge_workflow():
    task_object = GETask(
        name="test6",
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
        name="test7",
        data_source="data",
        inputs=kwtypes(dataset=str),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
        checkpoint_params={
            "site_names": ["local_site"],
        },
    )

    task_object.execute(dataset="yellow_tripdata_sample_2019-01.csv")


def test_ge_remote_flytefile():
    task_object = GETask(
        name="test8",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile[typing.TypeVar("csv")]),
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp",
    )

    task_object.execute(
        dataset="https://raw.githubusercontent.com/superconductive/ge_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )


def test_ge_remote_flytefile_task():
    task_object = GETask(
        name="test9",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile[typing.TypeVar("csv")]),
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp",
    )

    @task
    def my_task(dataset: FlyteFile[typing.TypeVar("csv")]) -> int:
        task_object.execute(dataset=dataset)
        return len(pd.read_csv(dataset))

    @workflow
    def my_wf(dataset: FlyteFile[typing.TypeVar("csv")]) -> int:
        return my_task(dataset=dataset)

    result = my_wf(
        dataset="https://raw.githubusercontent.com/superconductive/ge_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )
    assert result == 10000


def test_ge_local_flytefile():
    task_object = GETask(
        name="test10",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile[typing.TypeVar("csv")]),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    task_object.execute(dataset="yellow_tripdata_sample_2019-01.csv")


def test_ge_local_flytefile_task():
    task_object = GETask(
        name="test11",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile[typing.TypeVar("csv")]),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def my_task(dataset: FlyteFile[typing.TypeVar("csv")]) -> int:
        task_object.execute(dataset=dataset)
        return len(pd.read_csv(dataset))

    @workflow
    def my_wf(dataset: FlyteFile[typing.TypeVar("csv")]) -> int:
        return my_task(dataset=dataset)

    result = my_wf(dataset="data/yellow_tripdata_sample_2019-01.csv")
    assert result == 10000


def test_ge_local_flytefile_workflow():
    task_object = GETask(
        name="test12",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile[typing.TypeVar("csv")]),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    @workflow
    def valid_wf(dataset: FlyteFile[typing.TypeVar("csv")] = "data/yellow_tripdata_sample_2019-01.csv") -> None:
        task_object(dataset=dataset)

    valid_wf()


def test_ge_remote_flytefile_workflow():
    task_object = GETask(
        name="test13",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile[typing.TypeVar("csv")]),
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp",
    )

    @workflow
    def valid_wf(
        dataset: FlyteFile[
            typing.TypeVar("csv")
        ] = "https://raw.githubusercontent.com/superconductive/ge_tutorials/main/data/yellow_tripdata_sample_2019-01.csv",
    ) -> None:
        task_object(dataset=dataset)

    valid_wf()


def test_ge_flytefile_multiple_args():
    task_object_one = GETask(
        name="test14",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile),
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )
    task_object_two = GETask(
        name="test6",
        data_source="data",
        inputs=kwtypes(dataset=FlyteFile),
        expectation_suite="test1.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def get_file_name(dataset_one: FlyteFile, dataset_two: FlyteFile) -> (int, int):
        task_object_one(dataset=dataset_one)
        task_object_two(dataset=dataset_two)
        df_one = pd.read_csv(os.path.join("data", dataset_one))
        df_two = pd.read_csv(os.path.join("data", dataset_two))
        return len(df_one), len(df_two)

    @workflow
    def wf(
        dataset_one: FlyteFile = "data/yellow_tripdata_sample_2019-01.csv",
        dataset_two: FlyteFile = "data/yellow_tripdata_sample_2019-02.csv",
    ) -> (int, int):
        return get_file_name(dataset_one=dataset_one, dataset_two=dataset_two)

    assert wf() == (10000, 10000)


def test_ge_flyteschema():
    task_object = GETask(
        name="test15",
        data_source="data",
        inputs=kwtypes(dataset=FlyteSchema),
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp/test.parquet",
    )

    df = pd.read_csv("data/yellow_tripdata_sample_2019-01.csv")
    task_object(dataset=df)


def test_ge_flyteschema_task():
    task_object = GETask(
        name="test16",
        data_source="data",
        inputs=kwtypes(dataset=FlyteSchema),
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp/test1.parquet",
    )

    @task
    def my_task(dataframe: pd.DataFrame) -> int:
        task_object(dataset=dataframe)
        return dataframe.shape[0]

    @workflow
    def valid_wf(dataframe: pd.DataFrame) -> int:
        return my_task(dataframe=dataframe)

    df = pd.read_csv("data/yellow_tripdata_sample_2019-01.csv")
    result = valid_wf(dataframe=df)
    assert result == 10000


def test_ge_flyteschema_sqlite():
    task_object = GETask(
        name="test17",
        data_source="data",
        inputs=kwtypes(dataset=FlyteSchema),
        expectation_suite="sqlite.movies",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp/test1.parquet",
    )

    @task
    def my_task(dataset: FlyteSchema):
        task_object.execute(dataset=dataset)

    @workflow
    def my_wf(dataset: FlyteSchema):
        my_task(dataset=dataset)

    con = sqlite3.connect(os.path.join("data", "movies.sqlite"))
    df = pd.read_sql_query("SELECT * FROM movies", con)
    con.close()
    my_wf(dataset=df)


def test_ge_flyteschema_workflow():
    task_object = GETask(
        name="test18",
        data_source="data",
        inputs=kwtypes(dataset=FlyteSchema),
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp/test1.parquet",
    )

    @workflow
    def my_wf(dataframe: pd.DataFrame):
        task_object(dataset=dataframe)

    df = pd.read_csv("data/yellow_tripdata_sample_2019-01.csv")
    my_wf(dataframe=df)

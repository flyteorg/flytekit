import os
import sqlite3
import typing

import pandas as pd
import pytest
from flytekitplugins.greatexpectations import BatchConfig, GEConfig, GEType
from great_expectations.exceptions import ValidationError

from flytekit import task, workflow
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema

if "tests/greatexpectations" not in os.getcwd():
    os.chdir("plugins/tests/greatexpectations/")


def test_ge_type():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    s = GEType[str, ge_config]
    assert s.config()[1] == ge_config
    assert s.config()[1].data_source == "data"


def test_ge_schema_task():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def get_length(dataframe: pd.DataFrame) -> int:
        return dataframe.shape[0]

    @task
    def my_task(csv_file: GEType[str, ge_config]) -> pd.DataFrame:
        df = pd.read_csv(os.path.join("data", csv_file))
        df.drop(5, axis=0, inplace=True)
        return df

    @workflow
    def valid_wf(dataset: str = "yellow_tripdata_sample_2019-01.csv") -> int:
        dataframe = my_task(csv_file=dataset)
        return get_length(dataframe=dataframe)

    @workflow
    def invalid_wf(dataset: str = "yellow_tripdata_sample_2019-02.csv") -> int:
        dataframe = my_task(csv_file=dataset)
        return get_length(dataframe=dataframe)

    valid_result = valid_wf()
    assert valid_result == 9999

    with pytest.raises(ValidationError):
        invalid_wf()


def test_ge_schema_multiple_args():
    ge_config_one = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )
    ge_config_two = GEConfig(
        data_source="data",
        expectation_suite="test1.demo",
        data_connector="data_example_data_connector",
    )

    @task
    def get_file_name(dataset_one: GEType[str, ge_config_one], dataset_two: GEType[str, ge_config_two]) -> (int, int):
        df_one = pd.read_csv(os.path.join("data", dataset_one))
        df_two = pd.read_csv(os.path.join("data", dataset_two))
        return len(df_one), len(df_two)

    @workflow
    def wf(
        dataset_one: str = "yellow_tripdata_sample_2019-01.csv", dataset_two: str = "yellow_tripdata_sample_2019-02.csv"
    ) -> (int, int):
        return get_file_name(dataset_one=dataset_one, dataset_two=dataset_two)

    assert wf() == (10000, 10000)


def test_ge_schema_batchrequest_pandas_config():
    @task
    def my_task(
        directory: GEType[
            str,
            GEConfig(
                data_source="data",
                expectation_suite="test.demo",
                data_connector="my_data_connector",
                batchrequest_config=BatchConfig(
                    data_connector_query={
                        "batch_filter_parameters": {
                            "year": "2019",
                            "month": "01",  # noqa: F722
                        },
                    },
                    limit=10,
                ),
            ),
        ]
    ) -> str:
        return directory

    @workflow
    def my_wf():
        my_task(directory="my_assets")

    my_wf()


def test_invalid_ge_schema_batchrequest_pandas_config():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="my_data_connector",
        batchrequest_config=BatchConfig(
            data_connector_query={
                "batch_filter_parameters": {
                    "year": "2020",
                },
            }
        ),
    )

    @task
    def my_task(directory: GEType[str, ge_config]) -> str:
        return directory

    @workflow
    def my_wf():
        my_task(directory="my_assets")

    # Capture IndexError
    with pytest.raises(IndexError):
        my_wf()


def test_ge_schema_runtimebatchrequest_sqlite_config():
    ge_config = GEConfig(
        data_source="sqlite_data",
        expectation_suite="sqlite.movies",
        data_connector="sqlite_data_connector",
        batchrequest_config=BatchConfig(
            runtime_parameters={"query": "SELECT * FROM movies"},
            batch_identifiers={
                "pipeline_stage": "validation",
            },
        ),
    )

    @task
    def my_task(sqlite_db: GEType[str, ge_config]) -> int:
        # read sqlite query results into a pandas DataFrame
        con = sqlite3.connect(os.path.join("data", sqlite_db))
        df = pd.read_sql_query("SELECT * FROM movies", con)
        con.close()

        # verify that result of SQL query is stored in the dataframe
        return len(df)

    @workflow
    def my_wf() -> int:
        return my_task(sqlite_db="movies.sqlite")

    result = my_wf()
    assert result == 2736


def test_ge_schema_checkpoint_params():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
        checkpoint_params={
            "site_names": ["local_site"],
        },
    )

    @task
    def my_task(dataset: GEType[str, ge_config]) -> None:
        assert type(dataset) == str

    @workflow
    def my_wf() -> None:
        my_task(dataset="yellow_tripdata_sample_2019-01.csv")

    my_wf()


def test_ge_schema_flyteschema():
    @task
    def my_task(
        dataframe: GEType[
            FlyteSchema,
            GEConfig(
                data_source="data",
                expectation_suite="test.demo",
                data_connector="data_flytetype_data_connector",
                batchrequest_config=BatchConfig(limit=10),
                local_file_path="/tmp/test3.parquet",  # noqa: F722
            ),
        ]
    ) -> int:
        return dataframe.shape[0]

    @workflow
    def valid_wf(dataframe: FlyteSchema) -> int:
        return my_task(dataframe=dataframe)

    df = pd.read_csv("data/yellow_tripdata_sample_2019-01.csv")
    result = valid_wf(dataframe=df)
    assert result == 10000


def test_ge_schema_flyteschema_literal():
    @task
    def my_task(
        dataframe: GEType[
            FlyteSchema,
            GEConfig(
                data_source="data",
                expectation_suite="test.demo",
                data_connector="data_flytetype_data_connector",
                batchrequest_config=BatchConfig(limit=10),
                local_file_path="/tmp/test3.parquet",  # noqa: F722
            ),
        ]
    ) -> int:
        return dataframe.shape[0]

    @workflow
    def valid_wf() -> int:
        df = pd.read_csv("data/yellow_tripdata_sample_2019-01.csv")
        return my_task(dataframe=df)

    result = valid_wf()
    assert result == 10000


def test_ge_schema_remote_flytefile():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp",
    )

    @task
    def my_task(dataset: GEType[FlyteFile[typing.TypeVar("csv")], ge_config]) -> int:
        return len(pd.read_csv(dataset))

    @workflow
    def my_wf(dataset: FlyteFile[typing.TypeVar("csv")]) -> int:
        return my_task(dataset=dataset)

    result = my_wf(
        dataset="https://raw.githubusercontent.com/superconductive/ge_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )
    assert result == 10000


def test_ge_schema_remote_flytefile_literal():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
        local_file_path="/tmp",
    )

    @task
    def my_task(dataset: GEType[FlyteFile[typing.TypeVar("csv")], ge_config]) -> int:
        return len(pd.read_csv(dataset))

    @workflow
    def my_wf() -> int:
        return my_task(
            dataset="https://raw.githubusercontent.com/superconductive/ge_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        )

    result = my_wf()
    assert result == 10000


def test_ge_local_flytefile():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
    )

    @task
    def my_task(dataset: GEType[FlyteFile[typing.TypeVar("csv")], ge_config]) -> int:
        return len(pd.read_csv(dataset))

    @workflow
    def my_wf(dataset: FlyteFile[typing.TypeVar("csv")]) -> int:
        return my_task(dataset=dataset)

    result = my_wf(dataset="data/yellow_tripdata_sample_2019-01.csv")
    assert result == 10000


def test_ge_local_flytefile_literal():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_flytetype_data_connector",
    )

    @task
    def my_task(dataset: GEType[FlyteFile[typing.TypeVar("csv")], ge_config]) -> int:
        return len(pd.read_csv(dataset))

    @workflow
    def my_wf() -> int:
        return my_task(dataset="data/yellow_tripdata_sample_2019-01.csv")

    result = my_wf()
    assert result == 10000

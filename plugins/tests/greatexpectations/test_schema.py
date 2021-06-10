import os
import sqlite3
import tempfile

import pandas as pd
import pytest
from flytekitplugins.greatexpectations import BatchConfig, GEConfig, GEType
from great_expectations.exceptions import ValidationError

from flytekit import task, workflow


def test_ge_type():
    ge_config = GEConfig(
        data_source="data",
        expectation_suite="test.demo",
        data_connector="data_example_data_connector",
    )

    s = GEType[ge_config]
    assert s.config() == ge_config
    assert s.config().data_source == "data"


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
    def my_task(csv_file: GEType[ge_config]) -> pd.DataFrame:
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
    def get_file_name(dataset_one: GEType[ge_config_one], dataset_two: GEType[ge_config_two]) -> (int, int):
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
            GEConfig(
                data_source="data",
                expectation_suite="test.demo",
                data_connector="my_data_connector",
                batchrequest_config=BatchConfig(
                    data_connector_query={
                        "batch_filter_parameters": {
                            "year": "2019",
                            "month": "01",
                        },
                    },
                    limit=10,
                ),
            )
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
    def my_task(directory: GEType[ge_config]) -> str:
        return directory

    @workflow
    def my_wf():
        my_task(directory="my_assets")

    # Capture "ValueError: Got 0 batches instead of a single batch."
    with pytest.raises(ValueError, match=r".*0 batches.*"):
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
    def my_task(sqlite_db: GEType[ge_config]) -> int:
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
    def my_task(dataset: GEType[ge_config]) -> None:
        assert type(dataset) == str

    @workflow
    def my_wf() -> None:
        my_task(dataset="yellow_tripdata_sample_2019-01.csv")

    my_wf()

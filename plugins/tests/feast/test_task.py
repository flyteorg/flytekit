import typing
from datetime import datetime, timedelta

import pandas as pd
import pytest
from feast import ValueType
from flytekitplugins.feast import (
    DataSourceConfig,
    FeastOfflineRetrieveConfig,
    FeastOfflineRetrieveTask,
    FeastOfflineStoreConfig,
    FeastOfflineStoreTask,
    FeastOnlineRetrieveConfig,
    FeastOnlineRetrieveTask,
    FeastOnlineStoreConfig,
    FeastOnlineStoreTask,
    FeatureViewConfig,
)

from flytekit import kwtypes, workflow
from flytekit.types.schema.types import FlyteSchema


@pytest.fixture
def global_config():
    pytest.feature_view_conf = FeatureViewConfig(
        name="driver_hourly_stats_filesource",
        features={
            "conv_rate": ValueType.FLOAT,
            "acc_rate": ValueType.FLOAT,
            "avg_daily_trips": ValueType.INT64,
        },
        datasource="file",
        datasource_config=DataSourceConfig(
            event_timestamp_column="datetime", created_timestamp_column="created", local_file_path="/tmp/test.parquet"
        ),
    )
    pytest.feast_offline_store_conf = FeastOfflineStoreConfig(
        repo_path="feature_repo",
        entities=[("driver_id", ValueType.INT64)],
        feature_view=pytest.feature_view_conf,
    )

    pytest.feast_offline_retrieve_conf = FeastOfflineRetrieveConfig(
        entity_val=pd.DataFrame.from_dict(
            {
                "driver_id": [1001, 1002, 1003, 1004],
                "event_timestamp": [
                    datetime(2021, 4, 12, 10, 59, 42),
                    datetime(2021, 4, 12, 8, 12, 10),
                    datetime(2021, 4, 12, 16, 40, 26),
                    datetime(2021, 4, 12, 15, 1, 12),
                ],
            }
        ),
        features={pytest.feature_view_conf.name: ["conv_rate", "acc_rate", "avg_daily_trips"]},
    )

    pytest.feast_online_store_conf = FeastOnlineStoreConfig(
        start_date=datetime.utcnow() - timedelta(days=50),
        end_date=datetime.utcnow() - timedelta(minutes=10),
    )

    pytest.feast_online_retrieve_conf = FeastOnlineRetrieveConfig(
        features={
            pytest.feature_view_conf.name: [
                "conv_rate",
                "acc_rate",
                "avg_daily_trips",
            ]
        },
        entity_rows=[{"driver_id": 1001}],
    )


def test_feast_filesource_parquet_offline(global_config):
    # Parquet File -- Offline
    offline_store_task = FeastOfflineStoreTask(
        name="test1_offline_store_file", inputs=kwtypes(dataset=str), task_config=pytest.feast_offline_store_conf
    )

    repo_path = offline_store_task(dataset="feature_repo/data/driver_stats.parquet")

    offline_retrieve_task = FeastOfflineRetrieveTask(
        name="test1_offline_retrieve_file", task_config=pytest.feast_offline_retrieve_conf
    )
    dataframe_offline = offline_retrieve_task(repo_path=repo_path)

    # Parquet File -- Online
    online_store_task = FeastOnlineStoreTask(name="test1_online_store_file", task_config=pytest.feast_online_store_conf)

    repo_path_online = online_store_task(repo_path=repo_path)

    online_retrieve_task = FeastOnlineRetrieveTask(
        name="test1_online_retrieve_file", task_config=pytest.feast_online_retrieve_conf
    )
    dataframe_online = online_retrieve_task(repo_path=repo_path_online)

    assert repo_path == "feature_repo"
    assert offline_store_task.python_interface.inputs == {"dataset": str}
    assert (
        offline_retrieve_task.python_interface.inputs
        == online_store_task.python_interface.inputs
        == online_retrieve_task.python_interface.inputs
        == offline_store_task.python_interface.outputs
        == online_store_task.python_interface.outputs
        == {"repo_path": str}
    )
    assert offline_store_task.feature_view_name == "driver_hourly_stats_filesource"
    assert offline_store_task.datasource_name == "file"
    assert offline_retrieve_task.python_interface.outputs == {"dataframe": pd.DataFrame}
    assert online_retrieve_task.python_interface.outputs == {"dict": typing.Dict[typing.Any, typing.Any]}
    assert len(dataframe_offline) == len(pytest.feast_offline_retrieve_conf.entity_val) == 4
    assert len(dataframe_online["driver_id"]) == 1


def test_feast_filesource_schema_offline(global_config):
    # Schema -- Offline
    offline_store_task = FeastOfflineStoreTask(
        name="test2_offline_store_schema",
        inputs=kwtypes(dataset=FlyteSchema),
        task_config=pytest.feast_offline_store_conf,
    )
    repo_path = offline_store_task(dataset=pd.read_parquet("feature_repo/data/driver_stats.parquet"))

    offline_retrieve_task = FeastOfflineRetrieveTask(
        name="test2_offline_retrieve_schema", task_config=pytest.feast_offline_retrieve_conf
    )
    dataframe_offline = offline_retrieve_task(repo_path=repo_path)

    # Schema -- Online
    online_store_task = FeastOnlineStoreTask(
        name="test2_online_store_schema",
        task_config=pytest.feast_online_store_conf,
    )
    repo_path_online = online_store_task(repo_path=repo_path)

    online_retrieve_task = FeastOnlineRetrieveTask(
        name="test2_online_retrieve_schema", task_config=pytest.feast_online_retrieve_conf
    )
    dataframe_online = online_retrieve_task(repo_path=repo_path_online)

    assert repo_path == "feature_repo"
    assert offline_store_task.python_interface.inputs == {"dataset": FlyteSchema}
    assert (
        offline_retrieve_task.python_interface.inputs
        == online_store_task.python_interface.inputs
        == online_retrieve_task.python_interface.inputs
        == offline_store_task.python_interface.outputs
        == online_store_task.python_interface.outputs
        == {"repo_path": str}
    )
    assert offline_store_task.feature_view_name == "driver_hourly_stats_filesource"
    assert offline_store_task.datasource_name == "file"
    assert offline_retrieve_task.python_interface.outputs == {"dataframe": pd.DataFrame}
    assert online_retrieve_task.python_interface.outputs == {"dict": typing.Dict[typing.Any, typing.Any]}
    assert len(dataframe_offline) == len(pytest.feast_offline_retrieve_conf.entity_val) == 4
    assert len(dataframe_online["driver_id"]) == 1


def test_feast_filesource_schema_wf(global_config):
    offline_store_task = FeastOfflineStoreTask(
        name="test3_offline_store_file", inputs=kwtypes(dataset=str), task_config=pytest.feast_offline_store_conf
    )

    offline_retrieve_task = FeastOfflineRetrieveTask(
        name="test3_offline_retrieve_schema",
        task_config=FeastOfflineRetrieveConfig(
            entity_val=pd.DataFrame.from_dict(
                {
                    "driver_id": [1001, 1002, 1003, 1004],
                    "event_timestamp": [
                        datetime(2021, 4, 12, 10, 59, 42),
                        datetime(2021, 4, 12, 8, 12, 10),
                        datetime(2021, 4, 12, 16, 40, 26),
                        datetime(2021, 4, 12, 15, 1, 12),
                    ],
                }
            ),
            features={pytest.feature_view_conf.name: ["conv_rate", "acc_rate", "avg_daily_trips"]},
        ),
    )

    @workflow
    def feast_offline_workflow() -> pd.DataFrame:
        repo_path = offline_store_task(dataset="feature_repo/data/driver_stats.parquet")
        return offline_retrieve_task(repo_path=repo_path)

    df = feast_offline_workflow()
    assert len(df) == len(pytest.feast_offline_retrieve_conf.entity_val)

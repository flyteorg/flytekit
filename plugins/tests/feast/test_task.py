from datetime import datetime

import pandas as pd
import pytest
from feast import ValueType
from flytekitplugins.feast import (
    DataSourceConfig,
    FeastOfflineRetrieveConfig,
    FeastOfflineRetrieveTask,
    FeastOfflineStoreConfig,
    FeastOfflineStoreTask,
    FeatureViewConfig,
)

from flytekit import kwtypes, task, workflow
from flytekit.types.schema.types import FlyteSchema


@pytest.fixture
def global_config():
    feature_view_conf = FeatureViewConfig(
        name="driver_hourly_stats_file",
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
        feature_view=feature_view_conf,
    )

    pytest.feast_offline_retrieve_conf = FeastOfflineRetrieveConfig(
        repo_path="feature_repo",
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
        features={feature_view_conf.name: ["conv_rate", "acc_rate", "avg_daily_trips"]},
    )


def test_feast_filesource_offline(global_config):
    # File
    FeastOfflineStoreTask(
        name="test1_offline_store_file", inputs=kwtypes(dataset=str), task_config=pytest.feast_offline_store_conf
    )(dataset="feature_repo/data/driver_stats.parquet")

    dataframe_file = FeastOfflineRetrieveTask(
        name="test1_offline_retrieve_file", task_config=pytest.feast_offline_retrieve_conf
    )()

    # Schema
    FeastOfflineStoreTask(
        name="test1_offline_store_schema",
        inputs=kwtypes(dataset=FlyteSchema),
        task_config=pytest.feast_offline_store_conf,
    )(dataset=pd.read_parquet("feature_repo/data/driver_stats.parquet"))

    dataframe_schema = FeastOfflineRetrieveTask(
        name="test1_offline_retrieve_schema", task_config=pytest.feast_offline_retrieve_conf
    )()

    assert len(dataframe_file) == len(dataframe_schema) == len(pytest.feast_offline_retrieve_conf.entity_val)


def test_feast_filesource_schema_task(global_config):
    @task
    def offline_retrieve_task() -> int:
        task_obj = FeastOfflineRetrieveTask(
            name="test2_offline_retrieve_schema", task_config=pytest.feast_offline_retrieve_conf
        )
        return len(task_obj.execute())

    @workflow
    def feast_offline_workflow() -> int:
        return offline_retrieve_task()

    assert feast_offline_workflow() == len(pytest.feast_offline_retrieve_conf.entity_val)

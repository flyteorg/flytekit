import datetime
import os
import typing
from dataclasses import dataclass

import pandas as pd
from dataclasses_json import dataclass_json
from feast import BigQuerySource, Entity, Feature, FeatureStore, FeatureView, FileSource, ValueType
from feast.data_format import ParquetFormat
from google.protobuf.duration_pb2 import Duration

from flytekit import PythonInstanceTask
from flytekit.core.context_manager import FlyteContext
from flytekit.extend import Interface
from flytekit.types.schema import FlyteSchema


@dataclass_json
@dataclass
class DataSourceConfig(object):
    bigquery_type: typing.Literal["table", "query"] = "table"
    local_file_path: str = ""
    event_timestamp_column: typing.Optional[str] = ""
    created_timestamp_column: typing.Optional[str] = ""
    field_mapping: typing.Optional[typing.Dict[str, str]] = None
    date_partition_column: typing.Optional[str] = ""
    table_ref: typing.Optional[str] = None


@dataclass_json
@dataclass
class FeatureViewConfig(object):
    name: str
    features: typing.Dict[str, ValueType]
    datasource: typing.Literal["file", "bigquery"]
    datasource_config: DataSourceConfig
    tags: typing.Optional[typing.Dict[str, str]] = None
    ttl: typing.Optional[typing.Union[Duration, datetime.timedelta]] = None


@dataclass_json
@dataclass
class FeastOfflineStoreConfig(object):
    repo_path: str
    entities: typing.List[typing.Tuple[str, ValueType]]
    feature_view: FeatureViewConfig


@dataclass_json
@dataclass
class FeastOfflineRetrieveConfig(object):
    repo_path: str
    entity_val: typing.Union[pd.DataFrame, str]
    features: typing.Dict[str, typing.List[str]]


@dataclass_json
@dataclass
class FeastOnlineStoreConfig(object):
    repo_path: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    feature_view_names: typing.Optional[typing.List[FeatureViewConfig]] = None


@dataclass_json
@dataclass
class FeastOnlineRetrieveConfig(object):
    repo_path: str
    entity_rows: typing.List[typing.Dict[str, typing.Any]]
    features: typing.Dict[str, typing.List[str]]


def unfold_features(features_dict: typing.Dict[str, typing.List[str]]):
    feature_refs_list = []
    for feature_name, features in features_dict.items():
        feature_refs_list.extend(list(map(lambda x: feature_name + ":" + x, features)))
    return feature_refs_list


class FeastOfflineStoreTask(PythonInstanceTask[FeastOfflineStoreConfig]):

    _TASK_TYPE = "feast"

    def __init__(
        self, name: str, task_config: FeastOfflineStoreConfig, inputs: typing.Dict[str, typing.Type], **kwargs
    ):
        self._name = name
        self._feature_offline_store_config = task_config

        super(FeastOfflineStoreTask, self).__init__(
            name=name, task_type=self._TASK_TYPE, task_config=task_config, interface=Interface(inputs=inputs), **kwargs
        )

    def execute(self, **kwargs) -> typing.Any:

        if len(self.python_interface.inputs.keys()) != 1:
            raise RuntimeError("Expected one dataset argument")

        dataset = kwargs[list(self.python_interface.inputs.keys())[0]]
        datatype = list(self.python_interface.inputs.values())[0]

        if not issubclass(datatype, (FlyteSchema, str)):
            raise RuntimeError("'dataset' has to have FlyteSchema/str datatype")

        if issubclass(datatype, FlyteSchema):
            if not self._feature_offline_store_config.feature_view.datasource_config.local_file_path:
                raise ValueError("local_file_path is missing!")

            # FlyteSchema
            if type(dataset) is FlyteSchema:
                # copy parquet file to user-given directory
                FlyteContext.current_context().file_access.download_directory(
                    dataset.remote_path,
                    self._feature_offline_store_config.feature_view.datasource_config.local_file_path,
                )

            # DataFrame (Pandas, Spark, etc.)
            else:
                if not os.path.exists(
                    self._feature_offline_store_config.feature_view.datasource_config.local_file_path
                ):
                    os.makedirs(
                        self._feature_offline_store_config.feature_view.datasource_config.local_file_path, exist_ok=True
                    )

                schema = FlyteSchema(
                    local_path=self._feature_offline_store_config.feature_view.datasource_config.local_file_path,
                )
                writer = schema.open(type(dataset))
                writer.write(dataset)

            dataset = self._feature_offline_store_config.feature_view.datasource_config.local_file_path

        if self._feature_offline_store_config.feature_view.datasource == "file":
            if FlyteContext.current_context().file_access.is_remote(dataset):
                datasource = FileSource(
                    file_format=ParquetFormat(),
                    file_url=dataset,
                )
            else:
                datasource = FileSource(
                    path=dataset,
                )
            datasource.event_timestamp_column = (
                self._feature_offline_store_config.feature_view.datasource_config.event_timestamp_column
            )
            datasource.created_timestamp_column = (
                self._feature_offline_store_config.feature_view.datasource_config.created_timestamp_column
            )
            datasource.field_mapping = self._feature_offline_store_config.feature_view.datasource_config.field_mapping
            datasource.date_partition_column = (
                self._feature_offline_store_config.feature_view.datasource_config.date_partition_column
            )

        elif self._feature_offline_store_config.feature_view.datasource == "bigquery":
            if self._feature_offline_store_config.feature_view.datasource_config.bigquery_type == "table":
                datasource = BigQuerySource(
                    table_ref=dataset,
                )
            elif self._feature_offline_store_config.feature_view.datasource_config.bigquery_type == "query":
                datasource = BigQuerySource(
                    query=dataset,
                )
            else:
                raise ValueError(
                    f"Unknown bigquery_type: {self._feature_offline_store_config.feature_view.datasource_config.bigquery_type}"
                )
            datasource.event_timestamp_column = (
                self._feature_offline_store_config.feature_view.datasource_config.event_timestamp_column
            )
            datasource.created_timestamp_column = (
                self._feature_offline_store_config.feature_view.datasource_config.created_timestamp_column
            )
            datasource.field_mapping = self._feature_offline_store_config.feature_view.datasource_config.field_mapping
            datasource.date_partition_column = (
                self._feature_offline_store_config.feature_view.datasource_config.date_partition_column
            )
            datasource.table_ref = self._feature_offline_store_config.feature_view.datasource_config.table_ref
        else:
            raise ValueError(f"Unsupported datasource: {self._feature_offline_store_config.feature_view.datasource}")

        fs = FeatureStore(repo_path=self._feature_offline_store_config.repo_path)
        entities = [
            Entity(
                name=self._feature_offline_store_config.entity[0],
                value_type=self._feature_offline_store_config.entity[1],
            )
            for self._feature_offline_store_config.entity in self._feature_offline_store_config.entities
        ]

        feature_view = FeatureView(
            name=self._feature_offline_store_config.feature_view.name,
            entities=[
                self._feature_offline_store_config.entity[0]
                for self._feature_offline_store_config.entity in self._feature_offline_store_config.entities
            ],
            features=[
                Feature(name=feature_name, dtype=feature_type)
                for feature_name, feature_type in self._feature_offline_store_config.feature_view.features.items()
            ],
            input=datasource,
            ttl=self._feature_offline_store_config.feature_view.ttl,
            tags=self._feature_offline_store_config.feature_view.tags,
        )

        fs.apply([feature_view] + entities)


class FeastOfflineRetrieveTask(PythonInstanceTask[FeastOfflineRetrieveConfig]):

    _TASK_TYPE = "feast"

    def __init__(
        self,
        name: str,
        task_config: FeastOfflineRetrieveConfig,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        **kwargs,
    ):
        self._name = name
        self._feast_offline_retrieve_config = task_config

        super(FeastOfflineRetrieveTask, self).__init__(
            name=name, task_type=self._TASK_TYPE, task_config=task_config, interface=Interface(inputs=inputs), **kwargs
        )

    def execute(self, **kwargs) -> pd.DataFrame:
        fs = FeatureStore(repo_path=self._feast_offline_retrieve_config.repo_path)
        entity_val = self._feast_offline_retrieve_config.entity_val

        retrieval_job = fs.get_historical_features(
            entity_df=entity_val,
            feature_refs=unfold_features(self._feast_offline_retrieve_config.features),
        )
        print("YES!!!!\n\n\n\n\n")
        feature_data = retrieval_job.to_df()
        print(feature_data)
        return feature_data


class FeastOnlineStoreTask(PythonInstanceTask[FeastOnlineStoreConfig]):

    _TASK_TYPE = "feast"

    def __init__(self, name: str, task_config: FeastOnlineStoreConfig, **kwargs):
        self._name = name
        self._feast_online_store_config = task_config

        super(FeastOnlineStoreTask, self).__init__(
            name=name, task_type=self._TASK_TYPE, task_config=task_config, **kwargs
        )

    def execute(self, **kwargs) -> typing.Any:
        fs = FeatureStore(repo_path=self._feast_online_store_config.repo_path)
        fs.materialize(
            start_date=self._feast_online_store_config.start_date,
            end_date=self._feast_online_store_config.end_date,
            feature_views=[x.name for x in self._feast_online_store_config.feature_view_names],
        )


class FeastOnlineRetrieveTask(PythonInstanceTask[FeastOfflineRetrieveConfig]):

    _TASK_TYPE = "feast"

    def __init__(self, name: str, task_config: FeastOnlineRetrieveConfig, **kwargs):
        self._name = name
        self._feast_online_retrieve_config = task_config

        super(FeastOnlineRetrieveTask, self).__init__(
            name=name, task_type=self._TASK_TYPE, task_config=task_config, **kwargs
        )

    def execute(self, **kwargs) -> typing.Any:
        fs = FeatureStore(repo_path=self._feast_online_retrieve_config.repo_path)

        online_response = fs.get_online_features(
            unfold_features(self._feast_online_retrieve_config.features), self._feast_online_retrieve_config.entity_rows
        )
        online_response_dict = online_response.to_dict()
        return online_response_dict

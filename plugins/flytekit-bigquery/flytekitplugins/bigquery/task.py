from dataclasses import dataclass
from typing import Any, Dict, Optional, Type

from google.cloud import bigquery
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.extend import SerializationSettings, SQLTask
from flytekit.models import task as _task_model
from flytekit.types.schema import FlyteSchema


@dataclass
class BigQueryConfig(object):
    """
    BigQueryConfig should be used to configure a BigQuery Task.
    """

    ProjectID: str
    Location: Optional[str] = None
    QueryJobConfig: Optional[bigquery.QueryJobConfig] = None


class BigQueryTask(SQLTask[BigQueryConfig]):
    """
    This is the simplest form of a BigQuery Task, that can be used even for tasks that do not produce any output.
    """

    # This task is executed using the BigQuery handler in the backend.
    _TASK_TYPE = "bigquery_query_job_task"

    def __init__(
        self,
        name: str,
        query_template: str,
        task_config: Optional[BigQueryConfig],
        inputs: Optional[Dict[str, Type]] = None,
        output_schema_type: Optional[Type[FlyteSchema]] = None,
        **kwargs,
    ):
        """
        To be used to query BigQuery Tables.

        :param name: Name of this task, should be unique in the project
        :param query_template: The actual query to run. We use Flyte's Golang templating format for Query templating.
          Refer to the templating documentation
        :param task_config: BigQueryConfig object
        :param inputs: Name and type of inputs specified as an ordered dictionary
        :param output_schema_type: If some data is produced by this query, then you can specify the output schema type
        :param kwargs: All other args required by Parent type - SQLTask
        """
        outputs = None
        if output_schema_type is not None:
            outputs = {
                "results": output_schema_type,
            }
        super().__init__(
            name=name,
            task_config=task_config,
            query_template=query_template,
            inputs=inputs,
            outputs=outputs,
            task_type=self._TASK_TYPE,
            **kwargs,
        )
        self._output_schema_type = output_schema_type

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        config = {
            "Location": self.task_config.Location,
            "ProjectID": self.task_config.ProjectID,
        }
        if self.task_config.QueryJobConfig is not None:
            config.update(self.task_config.QueryJobConfig.to_api_repr()["query"])
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)

    def get_sql(self, settings: SerializationSettings) -> Optional[_task_model.Sql]:
        sql = _task_model.Sql(statement=self.query_template, dialect=_task_model.Sql.Dialect.ANSI)
        return sql

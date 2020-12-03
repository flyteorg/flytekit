from typing import Any, Dict, List, Optional, Type

from google.protobuf.json_format import MessageToDict

from flytekit.annotated.base_sql_task import SQLTask
from flytekit.annotated.context_manager import RegistrationSettings
from flytekit.annotated.task import metadata as task_metadata_creator
from flytekit.models import task as _task_model
from flytekit.models.qubole import HiveQuery, QuboleHiveJob
from flytekit.types.schema import FlyteSchema


class HiveTask(SQLTask):
    """
    Hive Task
    """

    def __init__(
        self,
        name: str,
        cluster_label: str,
        inputs: Dict[str, Type],
        query_template: str,
        metadata: Optional[_task_model.TaskMetadata] = None,
        output_schema_type: Optional[Type[FlyteSchema]] = None,
        tags: Optional[List[str]] = None,
        *args,
        **kwargs,
    ):
        outputs = None
        if output_schema_type is not None:
            outputs = {
                "results": output_schema_type,
            }
        super().__init__(
            name=name,
            metadata=metadata or task_metadata_creator(),
            query_template=query_template,
            inputs=inputs,
            outputs=outputs,
            task_type="hive",
            *args,
            **kwargs,
        )
        self._output_schema_type = output_schema_type
        self._cluster_label = cluster_label
        self._tags = tags or []

    @property
    def cluster_label(self) -> str:
        return self._cluster_label

    @property
    def output_schema_type(self) -> Type[FlyteSchema]:
        return self._output_schema_type

    @property
    def tags(self) -> List[str]:
        return self._tags

    def get_custom(self, settings: RegistrationSettings) -> Dict[str, Any]:
        # timeout_sec and retry_count will become deprecated, please use timeout and retry settings on the Task
        query = HiveQuery(query=self.query_template, timeout_sec=0, retry_count=0)
        job = QuboleHiveJob(query=query, cluster_label=self.cluster_label, tags=self.tags,)
        return MessageToDict(job.to_flyte_idl())


class HiveSelectTask(HiveTask):
    _HIVE_QUERY_FORMATTER = """
        {stage_query_str}

        CREATE TEMPORARY TABLE {{{{ .PerRetryUniqueKey }}}}_tmp AS {select_query_str};
        CREATE EXTERNAL TABLE {{{{ .PerRetryUniqueKey }}}} LIKE {{{{ .PerRetryUniqueKey }}}}_tmp STORED AS PARQUET;
        ALTER TABLE {{{{ .PerRetryUniqueKey }}}} SET LOCATION '{{{{ .RawOutputDataPrefix }}}}';

        INSERT OVERWRITE TABLE {{{{ .PerRetryUniqueKey }}}}
            SELECT *
            FROM {{{{ .PerRetryUniqueKey }}}}_tmp;
        DROP TABLE {{{{ .PerRetryUniqueKey }}}};
        """

    def __init__(
        self,
        name: str,
        cluster_label: str,
        inputs: Dict[str, Type],
        select_query: str,
        output_schema_type: Type[FlyteSchema],
        stage_query: Optional[str] = None,
        metadata: Optional[_task_model.TaskMetadata] = None,
        tags: Optional[List[str]] = None,
        *args,
        **kwargs,
    ):
        query_template = HiveSelectTask._HIVE_QUERY_FORMATTER.format(
            stage_query_str=stage_query or "", select_query_str=select_query.strip().strip(";")
        )
        super().__init__(
            name=name,
            cluster_label=cluster_label,
            inputs=inputs,
            query_template=query_template,
            metadata=metadata,
            output_schema_type=output_schema_type,
            tags=tags,
        )

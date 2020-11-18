from typing import Any, Callable, Dict, Type

from google.protobuf.json_format import MessageToDict

from flytekit.annotated.task import SQLTask, TaskPlugins, PythonTask
from flytekit.models import task as _task_model
from flytekit.models.qubole import QuboleHiveJob, HiveQuery
from flytekit.types.schema import FlyteSchema


class HiveTask(SQLTask):
    """
    Hive Task
    """
    def __init__(
        self,
        name: str,
        metadata: _task_model.TaskMetadata,
        cluster_label: str,
        inputs: Dict[str, Type],
        output_schema_type: Type[FlyteSchema],
        stage_query_template: str,
        query_template: str,
        *args,
        **kwargs,
    ):
        outputs = {
            "results": output_schema_type,
        }
        super().__init__(
            name=name,
            metadata=metadata,
            query_template=query_template,
            inputs=inputs,
            outputs=outputs,
            task_type="hive",
            *args,
            **kwargs,
        )
        self._output_schema_type = output_schema_type
        self._cluster_label = cluster_label

    @property
    def cluster_label(self) -> str:
        return self._cluster_label

    @property
    def output_schema_type(self):
        return self._output_schema_type

    def get_custom(self) -> Dict[str, Any]:
        query = HiveQuery()
        job = QuboleHiveJob(
            query=self.query_template,
            cluster_label=self
        )
        return MessageToDict(job.to_flyte_idl())

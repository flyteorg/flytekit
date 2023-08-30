from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Type

from flytekit.configuration import SerializationSettings
from flytekit.extend import SQLTask
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.models import task as _task_model
from flytekit.types.structured import StructuredDataset


@dataclass
class SleepConfig(object):
    ProjectID: str


class SleepTask(AsyncAgentExecutorMixin, SQLTask[SleepConfig]):
    _TASK_TYPE = "sleep"

    def __init__(
        self,
        name: str,
        task_config: Optional[SleepConfig] = SleepConfig(ProjectID="project"),
        inputs: Optional[Dict[str, Type]] = None,
        output_structured_dataset_type: Optional[Type[StructuredDataset]] = None,
        **kwargs,
    ):
        outputs = None
        if output_structured_dataset_type is not None:
            outputs = {
                "results": output_structured_dataset_type,
            }
        super().__init__(
            name=name,
            query_template="select * from flyte",
            task_config=task_config,
            inputs=inputs,
            outputs=outputs,
            task_type=self._TASK_TYPE,
            **kwargs,
        )
        self._output_structured_dataset_type = output_structured_dataset_type

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return asdict(self.task_config)

    def get_sql(self, settings: SerializationSettings) -> Optional[_task_model.Sql]:
        return _task_model.Sql(statement=self.query_template, dialect=_task_model.Sql.Dialect.ANSI)

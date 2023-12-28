from typing import Any, Optional, Type

from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, Resource
from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit import FlyteContextManager
from flytekit.core.external_api_task import ExternalApiTask
from flytekit.core.type_engine import TypeEngine
from flytekit.models.literals import LiteralMap

from .boto3_mixin import Boto3AgentMixin


class SyncBotoAgentTask(Boto3AgentMixin, ExternalApiTask):
    """A general purpose boto3 agent that can be used to call any boto3 method synchronously."""

    def __init__(self, name: str, config: dict[str, Any], service: str, region: Optional[str] = None, **kwargs):
        super().__init__(service=service, region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        method: str,
        output_result_type: Type,
        inputs: Optional[LiteralMap] = None,
        additional_args: Optional[dict[str, Any]] = None,
        region: Optional[str] = None,
    ):
        inputs = inputs or LiteralMap(literals={})
        result = self._call(
            method=method,
            config=task_template.custom["task_config"],
            inputs=inputs,
            task_template=task_template,
            additional_args=additional_args,
            region=region,
        )

        ctx = FlyteContextManager.current_context()

        outputs = LiteralMap(
            {
                "o0": TypeEngine.to_literal(
                    ctx,
                    result,
                    output_result_type,
                    TypeEngine.to_literal_type(output_result_type),
                )
            }
        ).to_flyte_idl()
        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))

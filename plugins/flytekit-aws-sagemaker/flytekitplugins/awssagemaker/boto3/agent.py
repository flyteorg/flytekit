from typing import Any, Optional, Type

from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, Resource
from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit import FlyteContextManager
from flytekit.core.external_api_task import ExternalApiTask
from flytekit.core.type_engine import TypeEngine
from flytekit.models.literals import LiteralMap
from flytekit.extend.backend.base_agent import get_agent_secret

from .mixin import Boto3AgentMixin


class SyncBotoAgentTask(Boto3AgentMixin, ExternalApiTask):
    """A general purpose boto3 agent that can be used to call any boto3 method synchronously."""

    def __init__(
        self, name: str, service: str, method: str, config: dict[str, Any], region: Optional[str] = None, **kwargs
    ):
        super().__init__(service=service, method=method, region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        output_result_type: Optional[Type] = None,
        inputs: Optional[LiteralMap] = None,
        additional_args: Optional[dict[str, Any]] = None,
        region: Optional[str] = None,
    ):
        inputs = inputs or LiteralMap(literals={})
        result = self._call(
            config=task_template.custom["task_config"],
            inputs=inputs,
            task_template=task_template,
            additional_args=additional_args,
            region=region,
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        outputs = None
        if result:
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

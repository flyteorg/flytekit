from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution
from typing_extensions import Annotated

from flytekit import FlyteContextManager, kwtypes
from flytekit.core import context_manager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentRegistry,
    Resource,
    SyncAgentBase,
)
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from .boto3_mixin import Boto3AgentMixin


# https://github.com/flyteorg/flyte/issues/4505
def convert_floats_with_no_fraction_to_ints(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = convert_floats_with_no_fraction_to_ints(value)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            data[i] = convert_floats_with_no_fraction_to_ints(item)
    elif isinstance(data, float) and data.is_integer():
        return int(data)
    return data


class BotoAgent(SyncAgentBase):
    """A general purpose boto3 agent that can be used to call any boto3 method."""

    name = "Boto Agent"

    def __init__(self):
        super().__init__(task_type_name="boto")

    async def do(
        self,
        task_template: TaskTemplate,
        output_prefix: str,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> Resource:
        custom = task_template.custom

        service = custom.get("service")
        raw_config = custom.get("config")
        convert_floats_with_no_fraction_to_ints(raw_config)
        config = raw_config
        region = custom.get("region")
        method = custom.get("method")
        images = custom.get("images")

        boto3_object = Boto3AgentMixin(service=service, region=region)

        result = await boto3_object._call(
            method=method,
            config=config,
            images=images,
            inputs=inputs,
        )

        outputs = {"result": {"result": None}}
        if result:
            ctx = FlyteContextManager.current_context()
            builder = ctx.with_file_access(
                FileAccessProvider(
                    local_sandbox_dir=ctx.file_access.local_sandbox_dir,
                    raw_output_prefix=output_prefix,
                    data_config=ctx.file_access.data_config,
                )
            )
            with context_manager.FlyteContextManager.with_context(builder) as new_ctx:
                outputs = LiteralMap(
                    literals={
                        "result": TypeEngine.to_literal(
                            new_ctx,
                            result,
                            Annotated[dict, kwtypes(allow_pickle=True)],
                            TypeEngine.to_literal_type(dict),
                        )
                    }
                )

        return Resource(phase=TaskExecution.SUCCEEDED, outputs=outputs)


AgentRegistry.register(BotoAgent())

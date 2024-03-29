from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution

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

    async def do(self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs) -> Resource:
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

        outputs = None
        if result:
            outputs = {"result": result}

        return Resource(phase=TaskExecution.SUCCEEDED, outputs=outputs)


AgentRegistry.register(BotoAgent())

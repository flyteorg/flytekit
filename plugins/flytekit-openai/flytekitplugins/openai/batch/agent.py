import logging
from dataclasses import dataclass
from typing import Optional

import cloudpickle

from flytekit import FlyteContextManager, lazy_module
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentRegistry,
    AsyncAgentBase,
    Resource,
    ResourceMeta,
)
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.types.file import FlyteFile

openai = lazy_module("openai")
OPENAI_API_KEY = "FLYTE_OPENAI_API_KEY"

states = {
    "Running": ["in_progress", "finalizing", "validating"],
    "Success": ["completed"],
    "Failed": ["failed", "cancelled", "cancelling", "expired"],
}


def get_key_by_value(dictionary, value):
    for key, values in dictionary.items():
        if value in values:
            return key
    return None


@dataclass
class BatchEndpointMetadata(ResourceMeta):
    openai_org: str
    batch_id: str

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "BatchEndpointMetadata":
        return cloudpickle.loads(data)


class BatchEndpointAgent(AsyncAgentBase):
    name = "OpenAI Batch Endpoint Agent"

    def __init__(self):
        super().__init__(
            task_type_name="batch-endpoint",
        )

    def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> BatchEndpointMetadata:
        ctx = FlyteContextManager.current_context()

        input_values = TypeEngine.literal_map_to_kwargs(
            ctx,
            inputs,
            {
                "input_file": FlyteFile,
                "metadata": Optional[dict],
            },
        )

        file_access = FlyteContextManager.current_context().file_access
        local_outputs_file = file_access.get_random_local_path()
        file_access.get_data(input_values["input_file"], local_outputs_file)

        custom = task_template.custom
        client = openai.OpenAI(
            organization=custom["openai_organization"],
            api_key=get_agent_secret(secret_key=OPENAI_API_KEY),
        )

        logger = logging.getLogger("httpx")
        logger.setLevel(logging.WARNING)

        # Upload file
        uploaded_file_obj = client.files.create(file=open(local_outputs_file, "rb"), purpose="batch")

        # Create batch
        result = client.batches.create(
            **custom["config"],
            input_file_id=uploaded_file_obj.get("id"),
            metadata=input_values["metadata"],
        )
        batch_id = result["id"]

        return BatchEndpointMetadata(batch_id=batch_id, openai_org=custom["openai_organization"])

    def get(self, resource_meta: BatchEndpointMetadata, **kwargs) -> Resource:
        client = openai.OpenAI(
            organization=resource_meta.openai_org,
            api_key=get_agent_secret(secret_key=OPENAI_API_KEY),
        )

        retrieved_result = client.batches.retrieve(resource_meta.batch_id)

        current_state = retrieved_result.get("status")
        flyte_phase = convert_to_flyte_phase(get_key_by_value(states, value=current_state))

        message = None
        if current_state in states["Failed"]:
            errors = retrieved_result.get("errors")
            if errors:
                message = errors.data.message

        result = None
        if current_state in states["Success"]:
            result = {
                "output_file_id": retrieved_result.get("output_file_id"),
                "metadata": dict,
            }

        return Resource(phase=flyte_phase, outputs=result, message=message)

    def delete(self, resource_meta: BatchEndpointMetadata, **kwargs):
        client = openai.AsyncOpenAI(
            organization=resource_meta.openai_org,
            api_key=get_agent_secret(secret_key=OPENAI_API_KEY),
        )

        client.batches.cancel(resource_meta.batch_id)


AgentRegistry.register(BatchEndpointAgent())

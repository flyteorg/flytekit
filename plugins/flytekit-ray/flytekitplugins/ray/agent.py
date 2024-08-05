import os
from dataclasses import dataclass
from typing import Optional

import anyscale
from anyscale.job.models import JobConfig

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class AnyscaleJobMetadata(ResourceMeta):
    job_id: str


class AnyscaleAgent(AsyncAgentBase):
    name = "Anyscale Agent"

    def __init__(self):
        super().__init__(task_type_name="anyscale", metadata_type=AnyscaleJobMetadata)

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> AnyscaleJobMetadata:
        print("task_template", task_template)
        container = task_template.container
        config = JobConfig(
            name="flyte-rag",
            image_uri=container.image,
            env_vars={
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN"),
                "OPEN_API_KEY": os.getenv("OPEN_API_KEY"),
                "PYTHONPATH": "/root:."
            },
            entrypoint=" ".join(container.args),
            max_retries=1,
            compute_config="flyte-rag:2",
        )

        job_id = anyscale.job.submit(config)

        return AnyscaleJobMetadata(job_id=job_id)

    async def get(self, resource_meta: AnyscaleJobMetadata, **kwargs) -> Resource:
        cur_phase = convert_to_flyte_phase(anyscale.job.status(id=resource_meta.job_id).state.value)
        return Resource(phase=cur_phase, message=None, log_links=None)

    async def delete(self, resource_meta: AnyscaleJobMetadata, **kwargs):
        anyscale.job.terminate(id=resource_meta.job_id)


AgentRegistry.register(AnyscaleAgent())

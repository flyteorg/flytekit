from dataclasses import dataclass
from typing import Optional

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
import anyscale
from anyscale.job.models import JobConfig


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
        config = JobConfig(
            name="flyte-rag",
            entrypoint="sleep 30",
            working_dir=".",
            max_retries=1,
            compute_config="flyte-rag"
        )

        job_id = anyscale.job.submit(config)

        return AnyscaleJobMetadata(job_id=job_id)

    async def get(self, resource_meta: AnyscaleJobMetadata, **kwargs) -> Resource:
        cur_phase = convert_to_flyte_phase(anyscale.job.status(id=resource_meta.job_id))

        return Resource(phase=cur_phase, message=None, log_links=None)

    async def delete(self, resource_meta: AnyscaleJobMetadata, **kwargs):
        anyscale.job.terminate(id=resource_meta.job_id)


AgentRegistry.register(AnyscaleAgent())

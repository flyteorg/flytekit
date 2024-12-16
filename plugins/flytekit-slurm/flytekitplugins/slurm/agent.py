"""
Slurm agent.
"""

import subprocess
from dataclasses import dataclass
from typing import Optional

from openapi_client import ApiClient as Client
from openapi_client import Configuration as Config
from openapi_client.api.slurm_api import SlurmApi
from openapi_client.models.slurm_v0041_post_job_submit_request import SlurmV0041PostJobSubmitRequest
from openapi_client.models.slurm_v0041_post_job_submit_request_job import SlurmV0041PostJobSubmitRequestJob

from flytekit import lazy_module
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

aiohttp = lazy_module("aiohttp")


def _gen_jwt_auth_token() -> str:
    """Generate an auth token for JWT authentication.

    Returns:
        auth_token: An auth token for JWT authentication.
    """
    CMD = ["scontrol", "token", "lifespan=9999"]
    auth_token = (
        subprocess.run(CMD, check=True, capture_output=True, text=True).stdout.strip().replace("SLURM_JWT=", "")
    )

    return auth_token


# Setup slurm API
api_cfg = Config(
    # Run slurmrestd locally now
    host="http://localhost:6820/",
    access_token=_gen_jwt_auth_token(),
)
slurm = SlurmApi(Client(api_cfg))


@dataclass
class SlurmJobMetadata(ResourceMeta):
    job_id: str


class SlurmAgent(AsyncAgentBase):
    """Slurm agent."""

    name = "Slurm Agent"

    def __init__(self):
        super(SlurmAgent, self).__init__(task_type_name="slurm", metadata_type=SlurmJobMetadata)

    def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        """
        Return a resource meta that can be used to get the status of the task.
        """
        slurm_config = task_template.custom["slurm_config"]

        # Define a simple slurm job
        job = SlurmV0041PostJobSubmitRequest(
            script=slurm_config["script"],  # Should use a better method
            job=SlurmV0041PostJobSubmitRequestJob(
                account=slurm_config["account"],  # flyte
                partition=slurm_config["partition"],  # debug
                name=slurm_config["name"],  # hello-slurm-agent
                environment=slurm_config["environment"],  # ["PATH=/bin/:/sbin/:/usr/bin/:/usr/sbin/"]
                current_working_directory=slurm_config["current_working_directory"],  # /tmp
            ),
        )

        response = slurm.slurm_v0041_post_job_submit(job)
        # async with aiohttp.ClientSession() as session:
        #     async with slurm.slurm_v0041_post_job_submit(job) as resp:
        #         response = await resp
        #         assert len(response.errors) == 0 and len(response.warnings) == 0, (
        #             "There exist errors or warnings."
        #         )

        return SlurmJobMetadata(job_id=str(response.job_id))

    def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        # async with aiohttp.ClientSession() as session:
        # async with slurm.slurm_v0041_get_job(job_id=resource_meta.job_id) as resp:
        # response = await resp
        response = slurm.slurm_v0041_get_job(job_id=resource_meta.job_id)

        job = response.jobs[-1]
        outputs = {
            "o0": {
                "partition": job.partition,
                "account": job.account,
                "name": job.name,
                "post_job_id": str(resource_meta.job_id),
                "get_job_id": job.job_id,
            }
        }

        return Resource(phase=convert_to_flyte_phase("done"), outputs=outputs)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs):
        """
        Delete the task. This call should be idempotent. It should raise an error if fails to delete the task.
        """
        pass


AgentRegistry.register(SlurmAgent())

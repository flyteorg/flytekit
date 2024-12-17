"""
Slurm agent.
"""

import os
from dataclasses import dataclass
from typing import Optional

import asyncssh

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

HOST = "localhost"
PORT = 2200


@dataclass
class SlurmJobMetadata(ResourceMeta):
    job_id: str


class SlurmAgent(AsyncAgentBase):
    """Slurm agent."""

    name = "Slurm Agent"

    def __init__(self):
        super(SlurmAgent, self).__init__(task_type_name="slurm", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        """
        Return a resource meta that can be used to get the status of the task.
        """
        slurm_config = task_template.custom["slurm_config"]

        # Construct the sbatch command
        cmd = ["sbatch"]
        for k, v in slurm_config.items():
            if k in ["local_path", "remote_path", "environment"]:
                continue

            cmd.extend([f"--{k}", v])
        cmd = " ".join(cmd)
        cmd += f" {slurm_config['remote_path']}"

        async with asyncssh.connect(host=HOST, port=PORT, password=os.environ["PASSWORD"], known_hosts=None) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.put(slurm_config["local_path"], slurm_config["remote_path"])

            # env is optional so far
            res = await conn.run(cmd, check=True, env=slurm_config["environment"])

        job_id = res.stdout.split()[-1]

        return SlurmJobMetadata(job_id=job_id)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        async with asyncssh.connect(host=HOST, port=PORT, password=os.environ["PASSWORD"], known_hosts=None) as conn:
            res = await conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        output_map = {}
        for o in res.stdout.split(" "):
            if len(o) == 0:
                continue

            o = o.split("=")
            if len(o) == 2:
                output_map[o[0].strip()] = o[1].strip()
        assert output_map["JobState"] == "COMPLETED"
        outputs = {"o0": output_map}

        return Resource(phase=convert_to_flyte_phase("done"), outputs=outputs)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs):
        """
        Delete the task. This call should be idempotent. It should raise an error if fails to delete the task.
        """
        pass


AgentRegistry.register(SlurmAgent())

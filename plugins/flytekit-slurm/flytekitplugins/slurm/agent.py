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


class SSHCfg:
    host = os.environ["SSH_HOST"]
    port = int(os.environ["SSH_PORT"])
    username = os.environ["SSH_USERNAME"]
    password = os.environ["SSH_PASSWORD"]


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
        """Submit a batch script to a remote Slurm cluster with `sbatch`"""
        slurm_conf = task_template.custom["slurm_conf"]
        assert len(slurm_conf) == 0, "Please leave slurm_conf untouched now."

        batch_script = task_template.custom["batch_script"]
        with open("/tmp/test.slurm", "w") as f:
            f.write(batch_script)

        local_path = task_template.custom["local_path"]
        remote_path = task_template.custom["remote_path"]
        # Maybe needs to add environ for python and slurm in task.py Slurm and get_custom

        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password, known_hosts=None
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.put(local_path, remote_path)
                await sftp.put("/tmp/test.slurm", "/tmp/test.slurm")

            res = await conn.run("sbatch /tmp/test.slurm", check=True)

        job_id = res.stdout.split()[-1]

        return SlurmJobMetadata(job_id=job_id)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        """Check the Slurm job status and return the specified outputs."""
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password, known_hosts=None
        ) as conn:
            res = await conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        output_map = {}
        for o in res.stdout.split(" "):
            if len(o) == 0:
                continue

            o = o.split("=")
            if len(o) == 2:
                output_map[o[0].strip()] = o[1].strip()

        job_state = output_map["JobState"].lower()
        outputs = {"o0": output_map}

        return Resource(phase=convert_to_flyte_phase(job_state), outputs=outputs)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs):
        """Cancel the Slurm job.

        Delete the task. This call should be idempotent. It should raise an error if fails to delete the task.
        """
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password, known_hosts=None
        ) as conn:
            _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


AgentRegistry.register(SlurmAgent())

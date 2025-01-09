from dataclasses import dataclass
from typing import Dict, Optional

import asyncssh
from asyncssh import SSHClientConnection

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class SlurmJobMetadata(ResourceMeta):
    """Slurm job metadata.

    Args:
        job_id: Slurm job id.
    """

    job_id: str
    slurm_host: str


class SlurmAgent(AsyncAgentBase):
    name = "Slurm Agent"

    # SSH connection pool for multi-host environment
    # _ssh_clients: Dict[str, SSHClientConnection]
    _conn: Optional[SSHClientConnection] = None

    def __init__(self) -> None:
        super(SlurmAgent, self).__init__(task_type_name="slurm", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        # Retrieve task config
        slurm_host = task_template.custom["slurm_host"]
        batch_script_path = task_template.custom["batch_script_path"]
        sbatch_conf = task_template.custom["sbatch_conf"]

        # Construct sbatch command for Slurm cluster
        cmd = _get_sbatch_cmd(sbatch_conf=sbatch_conf, batch_script_path=batch_script_path)

        # Run Slurm job
        if self._conn is None:
            await self._connect(slurm_host)
        res = await self._conn.run(cmd, check=True)

        # Retrieve Slurm job id
        job_id = res.stdout.split()[-1]

        return SlurmJobMetadata(job_id=job_id, slurm_host=slurm_host)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        await self._connect(resource_meta.slurm_host)
        res = await self._conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        # Determine the current flyte phase from Slurm job state
        job_state = "running"
        for o in res.stdout.split(" "):
            if "JobState" in o:
                job_state = o.split("=")[1].strip().lower()
        cur_phase = convert_to_flyte_phase(job_state)

        return Resource(phase=cur_phase)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        await self._connect(resource_meta.slurm_host)
        _ = await self._conn.run(f"scancel {resource_meta.job_id}", check=True)

    async def _connect(self, slurm_host: str) -> None:
        """Make an SSH client connection."""
        self._conn = await asyncssh.connect(host=slurm_host)


def _get_sbatch_cmd(sbatch_conf: Dict[str, str], batch_script_path: str) -> str:
    """Construct Slurm sbatch command.

    We assume all main scripts and dependencies are on Slurm cluster.

    Args:
        sbatch_conf: Options of srun command.
        batch_script_path: Absolute path of the batch script on Slurm cluster.

    Returns:
        cmd: Slurm sbatch command.
    """
    # Setup sbatch options
    cmd = ["sbatch"]
    for opt, val in sbatch_conf.items():
        cmd.extend([f"--{opt}", str(val)])

    # Assign the batch script to run
    cmd.append(batch_script_path)
    cmd = " ".join(cmd)

    return cmd


AgentRegistry.register(SlurmAgent())

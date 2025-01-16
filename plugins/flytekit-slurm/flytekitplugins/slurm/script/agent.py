import tempfile
from dataclasses import dataclass
from typing import Dict, List, Optional

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


class SlurmScriptAgent(AsyncAgentBase):
    name = "Slurm Script Agent"

    # SSH connection pool for multi-host environment
    # _ssh_clients: Dict[str, SSHClientConnection]
    _conn: Optional[SSHClientConnection] = None

    # Tmp remote path of the batch script
    REMOTE_PATH = "/tmp/echo_shell.slurm"

    # Dummy script content
    DUMMY_SCRIPT = "#!/bin/bash"

    def __init__(self) -> None:
        super(SlurmScriptAgent, self).__init__(task_type_name="slurm", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        # Retrieve task config
        slurm_host = task_template.custom["slurm_host"]
        batch_script_args = task_template.custom["batch_script_args"]
        sbatch_conf = task_template.custom["sbatch_conf"]

        # Construct sbatch command for Slurm cluster
        upload_script = False
        if "script" in task_template.custom:
            script = task_template.custom["script"]
            assert script != self.DUMMY_SCRIPT, "Please write the user-defined batch script content."

            batch_script_path = self.REMOTE_PATH
            upload_script = True
        else:
            # Assume the batch script is already on Slurm
            batch_script_path = task_template.custom["batch_script_path"]
        cmd = _get_sbatch_cmd(
            sbatch_conf=sbatch_conf, batch_script_path=batch_script_path, batch_script_args=batch_script_args
        )

        # Run Slurm job
        if self._conn is None:
            await self._connect(slurm_host)
        if upload_script:
            with tempfile.NamedTemporaryFile("w") as f:
                f.write(script)
                f.flush()
                async with self._conn.start_sftp_client() as sftp:
                    await sftp.put(f.name, self.REMOTE_PATH)
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


def _get_sbatch_cmd(sbatch_conf: Dict[str, str], batch_script_path: str, batch_script_args: List[str] = None) -> str:
    """Construct Slurm sbatch command.

    We assume all main scripts and dependencies are on Slurm cluster.

    Args:
        sbatch_conf: Options of srun command.
        batch_script_path: Absolute path of the batch script on Slurm cluster.
        batch_script_args: Additional args for the batch script on Slurm cluster.

    Returns:
        cmd: Slurm sbatch command.
    """
    # Setup sbatch options
    cmd = ["sbatch"]
    for opt, val in sbatch_conf.items():
        cmd.extend([f"--{opt}", str(val)])

    # Assign the batch script to run
    cmd.append(batch_script_path)

    # Add args if present
    if batch_script_args:
        for arg in batch_script_args:
            cmd.append(arg)

    cmd = " ".join(cmd)
    return cmd


AgentRegistry.register(SlurmScriptAgent())

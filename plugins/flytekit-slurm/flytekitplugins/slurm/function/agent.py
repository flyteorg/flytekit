import tempfile
from dataclasses import dataclass
from typing import Dict, Optional

import asyncssh
from asyncssh import SSHClientConnection

from flytekit import logger
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.extend.backend.utils import get_agent_secret

SLURM_PRIVATE_KEY = "FLYTE_SLURM_PRIVATE_KEY"

@dataclass
class SlurmJobMetadata(ResourceMeta):
    """Slurm job metadata.

    Args:
        job_id: Slurm job id.
        slurm_host: Host alias of the Slurm cluster.
    """

    job_id: str
    slurm_host: str


class SlurmFunctionAgent(AsyncAgentBase):
    name = "Slurm Function Agent"

    # SSH connection pool for multi-host environment
    _conn: Optional[SSHClientConnection] = None

    # Tmp remote path of the batch script
    REMOTE_PATH = "/tmp/task.slurm"

    def __init__(self) -> None:
        super(SlurmFunctionAgent, self).__init__(task_type_name="slurm_fn", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        # Retrieve task config
        slurm_host = task_template.custom["slurm_host"]
        sbatch_conf = task_template.custom["sbatch_conf"]
        script = task_template.custom["script"]

        # Construct command for Slurm cluster
        cmd, script = _get_sbatch_cmd_and_script(
            sbatch_conf=sbatch_conf,
            entrypoint=" ".join(task_template.container.args),
            script=script,
            batch_script_path=self.REMOTE_PATH,
        )


        logger.info("@@@ task_template.container.args:")
        logger.info(task_template.container.args)
        logger.info("@@@ Slurm Command: ")
        logger.info(cmd)
        logger.info("@@@ Batch script: ")
        logger.info(script)
        logger.info("@@@ OPENAI KEY")
        OPENAI_API_KEY = "FLYTE_OPENAI_API_KEY"
        logger.info(get_agent_secret(secret_key=OPENAI_API_KEY))
        logger.info("@@@ Slurm PRIVATE KEY")
        logger.info(get_agent_secret(secret_key=SLURM_PRIVATE_KEY))
        # We can add secret under ./slurm_private_key if key not found

        # Run Slurm job
        await self._connect(slurm_host)
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(script)
            f.flush()
            async with self._conn.start_sftp_client() as sftp:
                await sftp.put(f.name, self.REMOTE_PATH)
        res = await self._conn.run(cmd, check=True)

        # Retrieve Slurm job id
        job_id = res.stdout.split()[-1]
        logger.info("@@@ create slurm job id: " + job_id)

        return SlurmJobMetadata(job_id=job_id, slurm_host=slurm_host)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        await self._connect(resource_meta.slurm_host)
        res = await self._conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        # Determine the current flyte phase from Slurm job state
        job_state = "running"
        for o in res.stdout.split(" "):
            if "JobState" in o:
                job_state = o.split("=")[1].strip().lower()

        logger.info("@@@ GET PHASE: ")
        logger.info(str(job_state))
        cur_phase = convert_to_flyte_phase(job_state)
        return Resource(phase=cur_phase)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        await self._connect(resource_meta.slurm_host)
        _ = await self._conn.run(f"scancel {resource_meta.job_id}", check=True)

    async def _connect(self, slurm_host: str) -> None:
        """Make an SSH client connection."""
        self._conn = await asyncssh.connect(host=slurm_host)


def _get_sbatch_cmd_and_script(
    sbatch_conf: Dict[str, str],
    entrypoint: str,
    script: Optional[str] = None,
    batch_script_path: str = "/tmp/task.slurm",
) -> str:
    """Construct the Slurm sbatch command and the batch script content.

    Flyte entrypoint, pyflyte-execute, is run within a bash shell process.

    Args:
        sbatch_conf: Options of sbatch command.
        entrypoint: Flyte entrypoint.
        script: User-defined script where "{task.fn}" serves as a placeholder for the
            task function execution. Users should insert "{task.fn}" at the desired
            execution point within the script. If the script is not provided, the task
            function will be executed directly.
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

    if script is None:
        script = f"""#!/bin/bash
        {entrypoint}
        """
    else:
        script = script.replace("{task.fn}", entrypoint)

    cmd = " ".join(cmd)

    return cmd, script


AgentRegistry.register(SlurmFunctionAgent())

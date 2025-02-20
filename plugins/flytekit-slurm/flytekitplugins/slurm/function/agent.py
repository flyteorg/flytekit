import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Optional
import uuid
from asyncssh import SSHClientConnection

from flytekit import logger
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from ..ssh_utils import ssh_connect, SSHConfig

@dataclass
class SlurmJobMetadata(ResourceMeta):
    """Slurm job metadata.

    Args:
        job_id: Slurm job id.
        ssh_config: Options of SSH client connection. For available options, please refer to
            <newly-added-ssh-utils-file>
    """

    job_id: str
    ssh_config: Dict[str, Any]

@dataclass
class SlurmCluster:
    host: str
    username: Optional[str] = None

    def __hash__(self):
        return hash((self.host, self.username))

class SlurmFunctionAgent(AsyncAgentBase):
    name = "Slurm Function Agent"

    # SSH connection pool for multi-host environment
    _conn: Optional[SSHClientConnection] = None

    ssh_config_to_ssh_conn: Dict[SlurmCluster, SSHClientConnection] = {}

    async def _get_or_create_ssh_connection(self, ssh_config: Dict[str, Any]) -> SSHClientConnection:
        """Get existing SSH connection or create a new one if needed.

        Args:
            ssh_config: SSH configuration dictionary

        Returns:
            SSHClientConnection: Active SSH connection
        """
        host=ssh_config.get("host")
        username=ssh_config.get("username")
        ssh_cluster_config = SlurmCluster(host=host, username=username)
        if self.ssh_config_to_ssh_conn.get(ssh_cluster_config) is None:
            conn = await ssh_connect(ssh_config=ssh_config)
            self.ssh_config_to_ssh_conn[ssh_cluster_config] = conn
        else:
            conn = self.ssh_config_to_ssh_conn[ssh_cluster_config]
            try:
                await conn.run("echo [TEST] SSH connection", check=True)
            except Exception as e:
                logger.info(f"Re-establishing SSH connection due to error: {e}")
                conn = await ssh_connect(ssh_config=ssh_config)
                self.ssh_config_to_ssh_conn[ssh_cluster_config] = conn
        return conn

    def __init__(self) -> None:
        super(SlurmFunctionAgent, self).__init__(task_type_name="slurm_fn", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        unique_script_name = f"/tmp/task_{uuid.uuid4().hex}.slurm"

        # Retrieve task config
        ssh_config = task_template.custom["ssh_config"]
        sbatch_conf = task_template.custom["sbatch_conf"]
        script = task_template.custom["script"]

        # Construct command for Slurm cluster
        cmd, script = _get_sbatch_cmd_and_script(
            sbatch_conf=sbatch_conf,
            entrypoint=" ".join(task_template.container.args),
            script=script,
            batch_script_path=unique_script_name,
        )

        logger.info("@@@ task_template.container.args:")
        logger.info(task_template.container.args)
        logger.info("@@@ Slurm Command: ")
        logger.info(cmd)
        logger.info("@@@ Batch script: ")
        logger.info(script)

        # Run Slurm job
        conn = await self._get_or_create_ssh_connection(ssh_config)
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(script)
            f.flush()
            async with conn.start_sftp_client() as sftp:
                await sftp.put(f.name, unique_script_name)
        res = await conn.run(cmd, check=True)

        # Retrieve Slurm job id
        job_id = res.stdout.split()[-1]
        logger.info("@@@ create slurm job id: " + job_id)

        return SlurmJobMetadata(job_id=job_id, ssh_config=ssh_config)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        ssh_config = resource_meta.ssh_config
        conn = await self._get_or_create_ssh_connection(ssh_config)
        job_res = await conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        # Determine the current flyte phase from Slurm job state
        job_state = "running"
        for o in job_res.stdout.split(" "):
            if "JobState" in o:
                job_state = o.split("=")[1].strip().lower()
            elif "StdOut" in o:
                stdout_path = o.split("=")[1].strip()
                msg_res = await conn.run(f"cat {stdout_path}", check=True)
                msg = msg_res.stdout

        logger.info("@@@ GET PHASE: ")
        logger.info(str(job_state))
        cur_phase = convert_to_flyte_phase(job_state)

        return Resource(phase=cur_phase, message=msg)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        conn = await self._get_or_create_ssh_connection(resource_meta.ssh_config)
        _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


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

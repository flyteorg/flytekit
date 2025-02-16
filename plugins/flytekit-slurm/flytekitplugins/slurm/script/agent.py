import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from asyncssh import SSHClientConnection

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from ..ssh_utils import ssh_connect


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
        ssh_config = task_template.custom["ssh_config"]
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
        conn = await ssh_connect(ssh_config=ssh_config)
        if upload_script:
            with tempfile.NamedTemporaryFile("w") as f:
                f.write(script)
                f.flush()
                async with conn.start_sftp_client() as sftp:
                    await sftp.put(f.name, self.REMOTE_PATH)
        res = await conn.run(cmd, check=True)

        # Retrieve Slurm job id
        job_id = res.stdout.split()[-1]

        return SlurmJobMetadata(job_id=job_id, ssh_config=ssh_config)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        conn = await ssh_connect(ssh_config=resource_meta.ssh_config)
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
        cur_phase = convert_to_flyte_phase(job_state)

        return Resource(phase=cur_phase, message=msg)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        conn = await ssh_connect(ssh_config=resource_meta.ssh_config)
        _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


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

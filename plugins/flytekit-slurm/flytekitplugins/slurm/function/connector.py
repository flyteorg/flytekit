import json
import tempfile
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from asyncssh import SSHClientConnection
from asyncssh.sftp import SFTPNoSuchFile

from flytekit.extend.backend.base_connector import AsyncConnectorBase, ConnectorRegistry, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from ..ssh_utils import SlurmCluster, get_ssh_conn


@dataclass
class SlurmJobMetadata(ResourceMeta):
    """Slurm job metadata.

    Args:
        job_id: Slurm job id.
        ssh_config: Options of SSH client connection. For available options, please refer to
            the ssh_utils module.

    Attributes:
        job_id (str): Slurm job id.
        ssh_config (Dict[str, Union[str, List[str], Tuple[str, ...]]]): SSH configuration options
            for establishing client connections.
    """

    job_id: str
    ssh_config: Dict[str, Union[str, List[str], Tuple[str, ...]]]


class SlurmFunctionConnector(AsyncConnectorBase):
    name = "Slurm Function Connector"

    # SSH connection pool for multi-host environment
    slurm_cluster_to_ssh_conn: Dict[SlurmCluster, SSHClientConnection] = {}

    def __init__(self) -> None:
        super(SlurmFunctionConnector, self).__init__(task_type_name="slurm_fn", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        unique_script_name = f"/tmp/task_{uuid.uuid4().hex}.slurm"

        # Retrieve task config
        ssh_config = task_template.custom["ssh_config"]
        sbatch_config = task_template.custom["sbatch_config"]
        script = task_template.custom["script"]

        # Construct command for Slurm cluster
        cmd, script = _get_sbatch_cmd_and_script(
            sbatch_config=sbatch_config,
            entrypoint=" ".join(task_template.container.args),
            script=script,
            batch_script_path=unique_script_name,
        )

        # Run Slurm job
        conn = await get_ssh_conn(ssh_config=ssh_config, slurm_cluster_to_ssh_conn=self.slurm_cluster_to_ssh_conn)
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(script)
            f.flush()
            async with conn.start_sftp_client() as sftp:
                await sftp.put(f.name, unique_script_name)
        res = await conn.run(cmd, check=True)

        # Retrieve Slurm job id
        job_id = res.stdout.split()[-1]

        return SlurmJobMetadata(job_id=job_id, ssh_config=ssh_config)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        conn = await get_ssh_conn(
            ssh_config=resource_meta.ssh_config, slurm_cluster_to_ssh_conn=self.slurm_cluster_to_ssh_conn
        )
        job_res = await conn.run(f"scontrol --json show job {resource_meta.job_id}", check=True)
        job_info = json.loads(job_res.stdout)["jobs"][0]
        # Determine the current flyte phase from Slurm job state
        job_state = job_info["job_state"][0].strip().lower()
        cur_phase = convert_to_flyte_phase(job_state)

        # Read stdout of the Slurm job
        msg = ""
        async with conn.start_sftp_client() as sftp:
            with tempfile.NamedTemporaryFile("w+") as f:
                try:
                    await sftp.get(job_info["standard_output"], f.name)

                    msg = f.read()
                    logger.info(f"[SLURM STDOUT] {msg}")
                except SFTPNoSuchFile:
                    logger.debug("Standard output file path doesn't exist on the Slurm cluster.")

        return Resource(phase=cur_phase, message=msg)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        conn = await get_ssh_conn(
            ssh_config=resource_meta.ssh_config, slurm_cluster_to_ssh_conn=self.slurm_cluster_to_ssh_conn
        )
        _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


def _get_sbatch_cmd_and_script(
    sbatch_config: Dict[str, str],
    entrypoint: str,
    script: Optional[str] = None,
    batch_script_path: str = "/tmp/task.slurm",
) -> Tuple[str, str]:
    """Construct the Slurm sbatch command and the batch script content.

    Flyte entrypoint, pyflyte-execute, is run within a bash shell process.

    Args:
        sbatch_config (Dict[str, str]): Options of sbatch command.
        entrypoint (str): Flyte entrypoint.
        script (Optional[str], optional): User-defined script where "{task.fn}" serves as a placeholder for the
            task function execution. Users should insert "{task.fn}" at the desired
            execution point within the script. If the script is not provided, the task
            function will be executed directly. Defaults to None.
        batch_script_path (str, optional): Absolute path of the batch script on Slurm cluster.
            Defaults to "/tmp/task.slurm".

    Returns:
        Tuple[str, str]: A tuple containing:
            - cmd: Slurm sbatch command
            - script: The batch script content
    """
    # Setup sbatch options
    cmd = ["sbatch"]
    for opt, val in sbatch_config.items():
        cmd.extend([f"--{opt}", str(val)])

    # Assign the batch script to run
    cmd.append(batch_script_path)

    if script is None:
        script = f"""#!/bin/bash -i
        {entrypoint}
        """
    else:
        script = script.replace("{task.fn}", entrypoint)

    cmd = " ".join(cmd)

    return cmd, script


ConnectorRegistry.register(SlurmFunctionConnector())

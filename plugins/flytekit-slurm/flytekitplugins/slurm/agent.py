import os
from dataclasses import dataclass
from typing import Dict, Optional

import asyncssh

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


# Configure ssh info
class SSHCfg:
    host = os.environ["SSH_HOST"]
    port = int(os.environ["SSH_PORT"])
    username = os.environ["SSH_USERNAME"]
    password = os.environ["SSH_PASSWORD"]


@dataclass
class SlurmJobMetadata(ResourceMeta):
    """Slurm job metadata.

    Args:
        job_id: Slurm job id.
    """

    job_id: str


class SlurmAgent(AsyncAgentBase):
    name = "Slurm Agent"

    def __init__(self) -> None:
        super(SlurmAgent, self).__init__(task_type_name="slurm", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        # Retrieve task config
        srun_conf = task_template.custom["srun_conf"]

        # Construct srun command for Slurm cluster
        cmd = _get_srun_cmd(srun_conf=srun_conf, entrypoint=" ".join(task_template.container.args))

        # Run Slurm job
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password
        ) as conn:
            res = await conn.run(cmd, check=True)

        # Direct return for sbatch
        # job_id = res.stdout.split()[-1]
        # Use echo trick for srun
        job_id = res.stdout.strip()

        return SlurmJobMetadata(job_id=job_id)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password
        ) as conn:
            res = await conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        # Determine the current flyte phase from Slurm job state
        job_state = "running"
        for o in res.stdout.split(" "):
            if "JobState" in o:
                job_state = o.split("=")[1].strip().lower()
        cur_phase = convert_to_flyte_phase(job_state)

        # outputs.pb will be loaded without setting outputs object
        return Resource(phase=cur_phase)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password
        ) as conn:
            _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


def _get_srun_cmd(srun_conf: Dict[str, str], entrypoint: str) -> str:
    """Construct Slurm srun command.

    Flyte entrypoint, pyflyte-execute, is run within a bash shell process.

    Args:
        srun_conf: Options of srun command.
        entrypoint: Flyte entrypoint.

    Returns:
        cmd: Slurm srun command.
    """
    # Setup srun options
    cmd = ["srun"]
    for opt, val in srun_conf.items():
        cmd.extend([f"--{opt}", str(val)])

    cmd.extend(["bash", "-c"])
    cmd = " ".join(cmd)

    cmd += f""" '# Setup environment variables
        export PATH=$PATH:/opt/anaconda/anaconda3/bin;

        # Run pyflyte-execute in a pre-built conda env
        source activate dev;
        {entrypoint};

        # A trick to show Slurm job id on stdout
        echo $SLURM_JOB_ID;'
    """

    return cmd


AgentRegistry.register(SlurmAgent())

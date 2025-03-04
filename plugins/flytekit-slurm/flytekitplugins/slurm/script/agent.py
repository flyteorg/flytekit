import tempfile
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type

from asyncssh import SSHClientConnection

import flytekit
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.extras.tasks.shell import OutputLocation, _PythonFStringInterpolizer
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
        outputs: A dictionary mapping from the output variable name to the output location.
    """

    job_id: str
    ssh_config: Dict[str, Any]
    outputs: Dict[str, str]


class SlurmScriptAgent(AsyncAgentBase):
    name = "Slurm Script Agent"

    # SSH connection pool for multi-host environment
    # _ssh_clients: Dict[str, SSHClientConnection]
    _conn: Optional[SSHClientConnection] = None

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
        uniq_script_path = f"/tmp/task_{uuid.uuid4().hex}.slurm"
        outputs = {}

        # Retrieve task config
        ssh_config = task_template.custom["ssh_config"]
        batch_script_args = task_template.custom["batch_script_args"]
        sbatch_conf = task_template.custom["sbatch_conf"]

        # Construct sbatch command for Slurm cluster
        upload_script = False
        if "script" in task_template.custom:
            script = task_template.custom["script"]
            assert script != self.DUMMY_SCRIPT, "Please write the user-defined batch script content."
            script, outputs = self._interpolate_script(
                script,
                input_literal_map=inputs,
                python_input_types=task_template.custom["python_input_types"],
                output_locs=task_template.custom["output_locs"],
            )

            batch_script_path = uniq_script_path
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
                    await sftp.put(f.name, batch_script_path)
        res = await conn.run(cmd, check=True)

        # Retrieve Slurm job id
        job_id = res.stdout.split()[-1]

        return SlurmJobMetadata(job_id=job_id, ssh_config=ssh_config, outputs=outputs)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        conn = await ssh_connect(ssh_config=resource_meta.ssh_config)
        job_res = await conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        # Determine the current flyte phase from Slurm job state
        msg = ""
        job_state = "running"
        for o in job_res.stdout.split(" "):
            if "JobState" in o:
                job_state = o.split("=")[1].strip().lower()
            elif "StdOut" in o:
                stdout_path = o.split("=")[1].strip()
                msg_res = await conn.run(f"cat {stdout_path}", check=True)
                msg = msg_res.stdout
        cur_phase = convert_to_flyte_phase(job_state)

        return Resource(phase=cur_phase, message=msg, outputs=resource_meta.outputs)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        conn = await ssh_connect(ssh_config=resource_meta.ssh_config)
        _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)

    def _interpolate_script(
        self,
        script: str,
        input_literal_map: Optional[LiteralMap] = None,
        python_input_types: Optional[Dict[str, Type]] = None,
        output_locs: Optional[List[OutputLocation]] = None,
    ) -> Tuple[str, Dict[str, str]]:
        """Interpolate the user-defined script with the specified input and output arguments.

        Args:
            script: The user-defined script with placeholders for dynamic input and output values.
            input_literal_map: The input literal map.
            python_input_types: A dictionary of input names to types.
            output_locs: Output locations.

        Returns:
            A tuple (script, outputs), where script is the interpolated script, and outputs is a
            dictionary mapping from the output variable name to the output location.
        """
        input_kwargs = TypeEngine.literal_map_to_kwargs(
            flytekit.current_context(),
            lm=input_literal_map,
            python_types={} if python_input_types is None else python_input_types,
        )
        interpolizer = _PythonFStringInterpolizer()

        # Interpolate output locations with input values
        outputs = {}
        if output_locs is not None:
            for oloc in output_locs:
                outputs[oloc.var] = interpolizer.interpolate(oloc.location, inputs=input_kwargs)

        # Interpolate the script
        script = interpolizer.interpolate(script, inputs=input_kwargs, outputs=outputs)

        return script, outputs


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

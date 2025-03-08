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

from ..ssh_utils import SlurmCluster, get_ssh_conn


@dataclass
class SlurmJobMetadata(ResourceMeta):
    """
    Slurm job metadata.

    Attributes:
        job_id (str): Slurm job id.
        ssh_config (Dict[str, Any]): SSH connection configuration options.
        outputs (Dict[str, str]): Mapping from the output variable name to the output location.
    """

    job_id: str
    ssh_config: Dict[str, Any]
    outputs: Dict[str, str]


class SlurmScriptAgent(AsyncAgentBase):
    name = "Slurm Script Agent"

    # SSH connection pool for multi-host environment
    slurm_cluster_to_ssh_conn: Dict[SlurmCluster, SSHClientConnection] = {}

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
        conn = await get_ssh_conn(ssh_config=ssh_config, slurm_cluster_to_ssh_conn=self.slurm_cluster_to_ssh_conn)
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
        conn = await get_ssh_conn(
            ssh_config=resource_meta.ssh_config, slurm_cluster_to_ssh_conn=self.slurm_cluster_to_ssh_conn
        )
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
        conn = await get_ssh_conn(
            ssh_config=resource_meta.ssh_config, slurm_cluster_to_ssh_conn=self.slurm_cluster_to_ssh_conn
        )
        _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)

    def _interpolate_script(
        self,
        script: str,
        input_literal_map: Optional[LiteralMap] = None,
        python_input_types: Optional[Dict[str, Type]] = None,
        output_locs: Optional[List[OutputLocation]] = None,
    ) -> Tuple[str, Dict[str, str]]:
        """
        Interpolate the user-defined script with the specified input and output arguments.

        Args:
            script (str): The user-defined script with placeholders for dynamic input/output.
            input_literal_map (Optional[LiteralMap]): The Flyte LiteralMap of inputs.
            python_input_types (Optional[Dict[str, Type]]): Mapping of input names to their Python/typing types.
            output_locs (Optional[List[OutputLocation]]): List of output locations to be interpolated.

        Returns:
            Tuple[str, Dict[str, str]]:
                - A two-element tuple in which the first element is the interpolated script (str),
                and the second is a dictionary mapping each output variable name to its final location (str).
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
    """
    Construct the Slurm sbatch command.

    Args:
        sbatch_conf (Dict[str, str]): Slurm sbatch configuration options (e.g., partition, job-name, etc.).
        batch_script_path (str): Absolute path on the Slurm cluster of the script to run.
        batch_script_args (List[str], optional): Additional arguments to pass to the batch script.

    Returns:
        str: The sbatch command string that can be executed on the Slurm cluster.
    """
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

"""
Slurm task.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.extras.tasks.shell import OutputLocation, ShellTask


@dataclass
class Slurm(object):
    """Configure Slurm settings. Note that we focus on sbatch command now.

    Compared with spark, please refer to https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.SparkContext.html.

    Args:
        ssh_config: Options of SSH client connection. For available options, please refer to
            <newly-added-ssh-utils-file>
        sbatch_conf: Options of sbatch command. For available options, please refer to
            https://slurm.schedmd.com/sbatch.html.
        batch_script_args: Additional args for the batch script on Slurm cluster.
    """

    ssh_config: Dict[str, Any]
    sbatch_conf: Optional[Dict[str, str]] = None
    batch_script_args: Optional[List[str]] = None

    def __post_init__(self):
        if self.sbatch_conf is None:
            self.sbatch_conf = {}


# See https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses
@dataclass(kw_only=True)
class SlurmRemoteScript(Slurm):
    """Encounter collision if Slurm is shared btw SlurmTask and SlurmShellTask."""

    batch_script_path: str


class SlurmTask(AsyncAgentExecutorMixin, PythonTask[SlurmRemoteScript]):
    _TASK_TYPE = "slurm"

    def __init__(
        self,
        name: str,
        task_config: SlurmRemoteScript,
        **kwargs,
    ):
        super(SlurmTask, self).__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=task_config,
            # Dummy interface, will support this after discussion
            interface=Interface(None, None),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "ssh_config": self.task_config.ssh_config,
            "batch_script_path": self.task_config.batch_script_path,
            "batch_script_args": self.task_config.batch_script_args,
            "sbatch_conf": self.task_config.sbatch_conf,
        }


class SlurmShellTask(AsyncAgentExecutorMixin, ShellTask[Slurm]):
    _TASK_TYPE = "slurm"

    def __init__(
        self,
        name: str,
        task_config: Slurm,
        script: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
        output_locs: Optional[List[OutputLocation]] = None,
        **kwargs,
    ):
        self._inputs = inputs

        super(SlurmShellTask, self).__init__(
            name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            script=script,
            inputs=inputs,
            output_locs=output_locs,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "ssh_config": self.task_config.ssh_config,
            "batch_script_args": self.task_config.batch_script_args,
            "sbatch_conf": self.task_config.sbatch_conf,
            "script": self._script,
            "python_input_types": self._inputs,
            "output_locs": self._output_locs,
        }


TaskPlugins.register_pythontask_plugin(SlurmRemoteScript, SlurmTask)
TaskPlugins.register_pythontask_plugin(Slurm, SlurmShellTask)

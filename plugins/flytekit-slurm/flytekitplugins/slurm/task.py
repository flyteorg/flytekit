"""
Slurm task.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.extras.tasks.shell import ShellTask


@dataclass
class Slurm(object):
    """Configure Slurm settings. Note that we focus on sbatch command now.

    Compared with spark, please refer to https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.SparkContext.html.

    Args:
        slurm_host: Slurm host name. We assume there's no default Slurm host now.
        batch_script_path: Absolute path of the batch script on Slurm cluster.
        sbatch_conf: Options of sbatch command. For available options, please refer to
        batch_script_args: Additional args for the batch script on Slurm cluster.
            https://slurm.schedmd.com/sbatch.html.
    """

    slurm_host: str
    batch_script_path: str
    batch_script_args: Optional[List[str]] = None
    sbatch_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.sbatch_conf is None:
            self.sbatch_conf = {}


@dataclass
class SlurmShell(object):
    """Encounter collision if Slurm is shared btw SlurmTask and SlurmShellTask."""

    slurm_host: str
    batch_script_args: Optional[List[str]] = None
    sbatch_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.sbatch_conf is None:
            self.sbatch_conf = {}


class SlurmTask(AsyncAgentExecutorMixin, ShellTask[Slurm]):
    """
    Actual Plugin that transforms the local python code for execution within a slurm context...
    """

    _TASK_TYPE = "slurm"

    def __init__(
        self,
        name: str,
        task_config: Slurm,
        **kwargs,
    ):
        super(SlurmTask, self).__init__(
            name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            # Dummy script as a tmp workaround
            script="#!/bin/bash",
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "slurm_host": self.task_config.slurm_host,
            "batch_script_path": self.task_config.batch_script_path,
            "batch_script_args": self.task_config.batch_script_args,
            "sbatch_conf": self.task_config.sbatch_conf,
        }


class SlurmShellTask(AsyncAgentExecutorMixin, ShellTask[Slurm]):
    _TASK_TYPE = "slurm"

    def __init__(
        self,
        name: str,
        task_config: SlurmShell,
        script: Optional[str] = None,
        **kwargs,
    ):
        super(SlurmShellTask, self).__init__(
            name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            script=script,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "slurm_host": self.task_config.slurm_host,
            "batch_script_args": self.task_config.batch_script_args,
            "sbatch_conf": self.task_config.sbatch_conf,
            "script": self._script,
        }


TaskPlugins.register_pythontask_plugin(Slurm, SlurmTask)
TaskPlugins.register_pythontask_plugin(SlurmShell, SlurmShellTask)

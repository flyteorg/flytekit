"""
Slurm task.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

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
        sbatch_conf: Options of sbatch command. For available options, please refer to
            https://slurm.schedmd.com/sbatch.html.
    """

    slurm_host: str
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
        script: Optional[str] = None,
        # Support reading a script file in the future
        # script_file: Optional[str] = None,
        **kwargs,
    ):
        super(SlurmTask, self).__init__(
            name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            script=script,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "script": self._script,
            "slurm_host": self.task_config.slurm_host,
            "sbatch_conf": self.task_config.sbatch_conf,
        }


TaskPlugins.register_pythontask_plugin(Slurm, SlurmTask)

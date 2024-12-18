"""
Slurm task.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class Slurm(object):
    """Configure slurm command args.

    This configuration mainly focuses on `srun` or `sbatch`. Please refer to
    https://slurm.schedmd.com/srun.html and https://slurm.schedmd.com/sbatch.html.

    Note that batch_script can be written at will, so slurm_conf is left untouched now.
    """

    slurm_conf: Optional[Dict[str, str]] = None
    batch_script: Optional[str] = None
    local_path: Optional[str] = None
    remote_path: Optional[str] = None

    def __post_init__(self):
        if self.slurm_conf is None:
            self.slurm_conf = {}


class SlurmTask(AsyncAgentExecutorMixin, PythonTask):
    """Slurm task."""

    _TASK_TYPE = "slurm"

    def __init__(self, name: str, task_config: Slurm, **kwargs) -> None:
        """Initialize a slurm task.

        Args:
            name: Unique name of the task.
            slurm_config: Slurm job configuration.
        """
        # Set the batch script to submit to Slurm
        self._batch_script = task_config.batch_script

        # Set an example nested script (e.g., .py) to execute with the batch script
        # We use SFTP to put the local script to remote
        self._local_path = task_config.local_path
        self._remote_path = task_config.remote_path

        inputs = {"dummy": str}
        outputs = {"o0": Dict[str, Any]}

        super().__init__(
            self._TASK_TYPE,
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "slurm_conf": self.task_config.slurm_conf,
            "batch_script": self._batch_script,
            "local_path": self._local_path,
            "remote_path": self._remote_path,
        }

"""
Slurm task.
"""

from typing import Any, Dict

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin

# @dataclass
# class Slurm(object):
#     """Refer to SparkJob. I'll get it soon."""

#     slurm_conf: Optional[Dict[str, str]] = None

#     def __post_init__(self):
#         if self.slurm_conf is None:
#             self.slurm_conf= {}


class SlurmTask(AsyncAgentExecutorMixin, PythonTask):
    """Slurm task."""

    _TASK_TYPE = "slurm"

    def __init__(self, name: str, slurm_config: Dict[str, Any], **kwargs) -> None:
        """Initialize a slurm task.

        Args:
            name: Unique name of the task.
            slurm_config: Slurm job configuration.
        """
        task_config = {"slurm_config": slurm_config}

        # Define inputs/outputs interface?
        inputs = {"dummy": str}
        outputs = {"o0": float}

        super().__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "slurm_config": self.task_config["slurm_config"],
        }

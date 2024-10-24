"""
This Plugin adds the capability of transferring data using Skyplane to Flyte.
Skyplane allows for significantly faster and cheaper data transfers compared to traditional methods.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins

@dataclass
class SkyplaneJob:
    """
    Configuration for a Skyplane transfer job.
    
    Args:
        source: Source location from where data will be transferred.
        destination: Destination location where data will be transferred.
        options: Additional options for the Skyplane transfer.
    """

    source: str
    destination: str
    options: Optional[Dict[str, Any]] = field(default_factory=dict)


class SkyplaneFunctionTask(PythonFunctionTask[SkyplaneJob]):
    """
    A Flyte task that uses Skyplane to transfer data.
    """

    _SKYPLANE_JOB_TYPE = "skyplane"

    def __init__(self, task_config: SkyplaneJob, task_function: Callable, **kwargs):
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            task_type=self._SKYPLANE_JOB_TYPE,
            **kwargs,
        )

    def get_command(self, settings: SerializationSettings) -> List[str]:
        """
        Constructs the command to be executed for the Skyplane transfer.
        
        Args:
            settings: Serialization settings for the Flyte task.
        
        Returns:
            A list of command line arguments for Skyplane.
        """
        cmd = super().get_command(settings)
        skyplane_cmd = ["skyplane", "transfer", self.task_config.source, self.task_config.destination]
        
        # Add additional options if specified
        if self.task_config.options:
            for key, value in self.task_config.options.items():
                skyplane_cmd.append(f"--{key}={value}")

        return skyplane_cmd + cmd

# Register the Skyplane Plugin into the Flytekit core plugin system
TaskPlugins.register_pythontask_plugin(SkyplaneJob, SkyplaneFunctionTask)
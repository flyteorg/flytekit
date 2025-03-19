from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
from flytekit.extras.tasks.shell import OutputLocation
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


@dataclass
class SlurmConfig(object):
    """
    Configure Slurm settings. Note that we focus on sbatch command now.

    Compared with spark, please refer to:
    https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.SparkContext.html.

    Attributes:
        ssh_config (Dict[str, Any]): Options of SSH client connection. For available options, please refer to
            <newly-added-ssh-utils-file>.
        sbatch_config (Optional[Dict[str, str]]): Options of sbatch command. For available options, please refer to
            https://slurm.schedmd.com/sbatch.html.
        batch_script_args (Optional[List[str]]): Additional args for the batch script on Slurm cluster.
    """

    ssh_config: Dict[str, Any]
    sbatch_config: Optional[Dict[str, str]] = None
    batch_script_args: Optional[List[str]] = None

    def __post_init__(self):
        if self.sbatch_config is None:
            self.sbatch_config = {}


@dataclass
class SlurmScriptConfig(SlurmConfig):
    """Encounter collision if Slurm is shared btw SlurmTask and SlurmShellTask."""

    batch_script_path: str = field(default=None)

    def __post_init__(self):
        super().__post_init__()
        if self.batch_script_path is None:
            raise ValueError("batch_script_path must be provided")


class SlurmTask(AsyncConnectorExecutorMixin, PythonTask[SlurmScriptConfig]):
    _TASK_TYPE = "slurm"

    def __init__(
        self,
        name: str,
        task_config: SlurmScriptConfig,
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
            "sbatch_config": self.task_config.sbatch_config,
        }


class SlurmShellTask(AsyncConnectorExecutorMixin, PythonTask[SlurmConfig]):
    _TASK_TYPE = "slurm"

    def __init__(
        self,
        name: str,
        task_config: SlurmConfig,
        script: str,
        inputs: Optional[Dict[str, Type]] = None,
        output_locs: Optional[List[OutputLocation]] = None,
        **kwargs,
    ):
        self._inputs = inputs
        self._output_locs = output_locs if output_locs is not None else []
        self._script = script

        outputs = self._validate_output_locs()

        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def _validate_output_locs(self) -> Dict[str, Type]:
        outputs = {}
        for v in self._output_locs:
            if v is None:
                raise ValueError("OutputLocation cannot be none")
            if not isinstance(v, OutputLocation):
                raise ValueError("Every output type should be an output location on the file-system")
            if v.location is None:
                raise ValueError(f"Output Location not provided for output var {v.var}")
            if not issubclass(v.var_type, FlyteFile) and not issubclass(v.var_type, FlyteDirectory):
                raise ValueError(
                    "Currently only outputs of type FlyteFile/FlyteDirectory and their derived types are supported"
                )
            outputs[v.var] = v.var_type
        return outputs

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "ssh_config": self.task_config.ssh_config,
            "batch_script_args": self.task_config.batch_script_args,
            "sbatch_config": self.task_config.sbatch_config,
            "script": self._script,
            "python_input_types": self._inputs,
            "output_locs": self._output_locs,
        }

    @property
    def script(self) -> str:
        return self._script


TaskPlugins.register_pythontask_plugin(SlurmScriptConfig, SlurmTask)
TaskPlugins.register_pythontask_plugin(SlurmConfig, SlurmShellTask)

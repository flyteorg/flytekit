import enum
import typing
from dataclasses import dataclass
from typing import Optional, Dict, Any

from google.protobuf.json_format import MessageToDict

from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.extend import Interface
from flytekit.models.task import SparkJob
from flytekit.sdk.spark_types import SparkType
from .task import PysparkFunctionTask


class JobType(enum.Enum):
    Scala = SparkType.SCALA
    Java = SparkType.JAVA
    python = SparkType.PYTHON


@dataclass
class GenericSparkConf(object):
    main_class: str
    main_application_file: str
    job_type: JobType = JobType.Scala
    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.spark_conf is None:
            self.spark_conf = {}

        if self.hadoop_conf is None:
            self.hadoop_conf = {}


class GenericSparkTask(PythonInstanceTask[GenericSparkConf]):

    def __init__(self, name: str, task_config: GenericSparkConf,
                 inputs: typing.Optional[typing.Dict[str, typing.Type]], **kwargs):
        super().__init__(name=name, task_config=task_config, task_type=PysparkFunctionTask._SPARK_TASK_TYPE,
                         interface=Interface(inputs=inputs), **kwargs)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = SparkJob(
            spark_conf=self.task_config.spark_conf,
            hadoop_conf=self.task_config.hadoop_conf,
            application_file="local://" + settings.entrypoint_settings.path if settings.entrypoint_settings else "",
            executor_path=settings.python_interpreter,
            main_class=self.task_config.main_class,
            spark_type=self.task_config.job_type,
        )
        return MessageToDict(job.to_flyte_idl())

    def get_command(self, settings: SerializationSettings) -> typing.List[str]:
        args = []
        for k, _ in self.interface.inputs.items():
            args.append(f"--{k}")
            args.append(f"{{.Inputs.{k}}}")
        return args

    def execute(self, **kwargs) -> Any:
        raise ValueError("")

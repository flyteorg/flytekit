import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.image_spec import ImageSpec

from .models import SparkJob, SparkType


@dataclass
class GenericSparkConf(object):
    main_class: str
    applications_path: str
    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.spark_conf is None:
            self.spark_conf = {}

        if self.hadoop_conf is None:
            self.hadoop_conf = {}


class GenericSparkTask(PythonFunctionTask[GenericSparkConf]):
    def __init__(
        self,
        task_config: GenericSparkConf,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        super(GenericSparkTask, self).__init__(
            task_config=task_config,
            task_type="spark",
            task_function=task_function,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = SparkJob(
            spark_conf=self.task_config.spark_conf,
            hadoop_conf=self.task_config.hadoop_conf,
            application_file="local://" + self.task_config.applications_path,
            executor_path=settings.python_interpreter,
            main_class=self.task_config.main_class,
            spark_type=SparkType.PYTHON,
        )
        return MessageToDict(job.to_flyte_idl())

    def execute(self):
        spark_submit_cmd = f"spark-submit  --class {self.task_config.main_class}  --master 'local[*]' {self.task_config.applications_path}"
        subprocess.run(spark_submit_cmd, shell=True, check=True)


# Inject the Spark plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(GenericSparkConf, GenericSparkTask)

import dataclasses
import os
import shutil
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union, cast, List

import click
from google.protobuf.json_format import MessageToDict

from flytekit import FlyteContextManager, PythonFunctionTask, lazy_module, logger
from flytekit.configuration import DefaultImages, SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.pod_template import PRIMARY_CONTAINER_DEFAULT_NAME, PodTemplate
from flytekit.extend import ExecutionState, TaskPlugins
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
from flytekit.image_spec import DefaultImageBuilder, ImageSpec
from flytekit.models.task import K8sPod

from .models import SparkJob, SparkType

pyspark_sql = lazy_module("pyspark.sql")
SparkSession = pyspark_sql.SparkSession


@dataclass
class GenericSparkConf(object):
    main_class: str
    executor_path: Optional[str] = None
    applications_path: Optional[str] = None
    spark_type: SparkType = SparkType.JAVA
    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.spark_conf is None:
            self.spark_conf = {}

        if self.hadoop_conf is None:
            self.hadoop_conf = {}


class GenericSparkTask(PythonFunctionTask[GenericSparkConf]):

    _SPARK_TASK_TYPE = "spark"

    def __init__(
        self,
        task_config: GenericSparkConf,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):

        self.sess: Optional[SparkSession] = None
        self._default_executor_path: str = task_config.executor_path
        self._default_applications_path: str = task_config.applications_path
        self._container_image = container_image
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
            application_file=f"local://" + self.task_config.applications_path,
            executor_path=settings.python_interpreter,
            main_class=self.task_config.main_class,
            spark_type=SparkType.PYTHON,
        )
        return MessageToDict(job.to_flyte_idl())

    # def get_command(self, settings: SerializationSettings) -> List[str]:
    #     args = []
    #     for k, _ in self.interface.inputs.items():
    #         args.append(f"--{k}")
    #         args.append(f"{{.Inputs.{k}}}")
    #     return args

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        import pyspark as _pyspark

        ctx = FlyteContextManager.current_context()
        sess_builder = _pyspark.sql.SparkSession.builder.appName(
            f"FlyteSpark: {user_params.execution_id}"
        )
        if not (
            ctx.execution_state
            and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION
        ):
            # If either of above cases is not true, then we are in local execution of this task
            # Add system spark-conf for local/notebook based execution.
            spark_conf = _pyspark.SparkConf()
            spark_conf.set("spark.driver.bindAddress", "127.0.0.1")
            for k, v in self.task_config.spark_conf.items():
                spark_conf.set(k, v)
            # In local execution, propagate PYTHONPATH to executors too. This makes the spark
            # execution hermetic to the execution environment. For example, it allows running
            # Spark applications using Bazel, without major changes.
            if "PYTHONPATH" in os.environ:
                spark_conf.setExecutorEnv("PYTHONPATH", os.environ["PYTHONPATH"])
            sess_builder = sess_builder.config(conf=spark_conf)

        self.sess = sess_builder.getOrCreate()

        if (
            ctx.serialization_settings
            and ctx.serialization_settings.fast_serialization_settings
            and ctx.serialization_settings.fast_serialization_settings.enabled
            and ctx.execution_state
            and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION
        ):
            file_name = "flyte_wf"
            file_format = "zip"
            shutil.make_archive(file_name, file_format, os.getcwd())
            self.sess.sparkContext.addPyFile(f"{file_name}.{file_format}")

        return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()

    def execute(self, **kwargs) -> Any:
        return PythonFunctionTask.execute(self, **kwargs)


# Inject the Spark plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(GenericSparkConf, GenericSparkTask)

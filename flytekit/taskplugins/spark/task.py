import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from google.protobuf.json_format import MessageToDict

from flytekit.annotated.context_manager import RegistrationSettings
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.task import TaskPlugins
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.models import task as _task_model
from flytekit.sdk.spark_types import SparkType


@dataclass
class Spark(object):
    """
    Use this to configure a SparkContext for a your task. Task's marked with this will automatically execute
    natively onto K8s as a distributed execution of spark

    Args:
        spark_conf: Dictionary of spark config. The variables should match what spark expects
        hadoop_conf: Dictionary of hadoop conf. The variables should match a typical hadoop configuration for spark
    """

    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None


@contextmanager
def new_spark_session(name: str):
    import pyspark as _pyspark

    sess = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {name}").getOrCreate()
    yield sess
    # SparkSession.Stop does not work correctly, as it stops the session before all the data is written
    # sess.stop()


class PysparkFunctionTask(PythonFunctionTask[Spark]):
    """
    Actual Plugin that transforms the local python code for execution within a spark context
    """

    _SPARK_TASK_TYPE = "spark"

    def __init__(self, task_config: Spark, task_function: Callable, **kwargs):
        super(PysparkFunctionTask, self).__init__(
            task_config=task_config, task_type=self._SPARK_TASK_TYPE, task_function=task_function, **kwargs,
        )

    def get_custom(self, settings: RegistrationSettings) -> Dict[str, Any]:
        from flytekit.bin import entrypoint as _entrypoint

        spark_exec_path = os.path.abspath(_entrypoint.__file__)
        if spark_exec_path.endswith(".pyc"):
            spark_exec_path = spark_exec_path[:-1]

        job = _task_model.SparkJob(
            spark_conf=self.task_config.spark_conf,
            hadoop_conf=self.task_config.hadoop_conf,
            application_file="local://" + spark_exec_path,
            executor_path=sys.executable,
            main_class="",
            spark_type=SparkType.PYTHON,
        )
        return MessageToDict(job.to_flyte_idl())

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        import pyspark as _pyspark

        sess = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {user_params.execution_id}").getOrCreate()
        return user_params.builder().add_attr("SPARK_SESSION", sess).build()


# Inject the Spark plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(Spark, PysparkFunctionTask)

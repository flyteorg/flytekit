import os
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from google.protobuf.json_format import MessageToDict
from pyspark.sql import SparkSession

from flytekit import FlyteContextManager, PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.extend import ExecutionState, TaskPlugins

from .models import SparkJob, SparkType


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

    def __post_init__(self):
        if self.spark_conf is None:
            self.spark_conf = {}

        if self.hadoop_conf is None:
            self.hadoop_conf = {}


# This method does not reset the SparkSession since it's a bit hard to handle multiple
# Spark sessions in a single application as it's described in:
# https://stackoverflow.com/questions/41491972/how-can-i-tear-down-a-sparksession-and-create-a-new-one-within-one-application.
def new_spark_session(name: str, conf: typing.Dict[str, str] = None):
    """
    Optionally creates a new spark session and returns it.
    In cluster mode (running in hosted flyte, this will disregard the spark conf passed in)

    This method is safe to be used from any other method. That is one reason why, we have duplicated this code
    fragment with the pre-execute. For example in the notebook scenario we might want to call it from a separate kernel
    """
    import pyspark as _pyspark

    # We run in cluster-mode in Flyte.
    # Ref https://github.com/lyft/flyteplugins/blob/master/go/tasks/v1/flytek8s/k8s_resource_adds.go#L46
    sess_builder = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {name}")
    if "FLYTE_INTERNAL_EXECUTION_ID" not in os.environ and conf is not None:
        # If either of above cases is not true, then we are in local execution of this task
        # Add system spark-conf for local/notebook based execution.
        sess_builder = sess_builder.master("local[*]")
        spark_conf = _pyspark.SparkConf()
        for k, v in conf.items():
            spark_conf.set(k, v)
        spark_conf.set("spark.driver.bindAddress", "127.0.0.1")
        # In local execution, propagate PYTHONPATH to executors too. This makes the spark
        # execution hermetic to the execution environment. For example, it allows running
        # Spark applications using Bazel, without major changes.
        if "PYTHONPATH" in os.environ:
            spark_conf.setExecutorEnv("PYTHONPATH", os.environ["PYTHONPATH"])
        sess_builder = sess_builder.config(conf=spark_conf)

    # If there is a global SparkSession available, get it and try to stop it.
    _pyspark.sql.SparkSession.builder.getOrCreate().stop()

    return sess_builder.getOrCreate()
    # SparkSession.Stop does not work correctly, as it stops the session before all the data is written
    # sess.stop()


class PysparkFunctionTask(PythonFunctionTask[Spark]):
    """
    Actual Plugin that transforms the local python code for execution within a spark context
    """

    _SPARK_TASK_TYPE = "spark"

    def __init__(self, task_config: Spark, task_function: Callable, **kwargs):
        super(PysparkFunctionTask, self).__init__(
            task_config=task_config,
            task_type=self._SPARK_TASK_TYPE,
            task_function=task_function,
            **kwargs,
        )
        self.sess: Optional[SparkSession] = None

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = SparkJob(
            spark_conf=self.task_config.spark_conf,
            hadoop_conf=self.task_config.hadoop_conf,
            application_file="local://" + settings.entrypoint_settings.path,
            executor_path=settings.python_interpreter,
            main_class="",
            spark_type=SparkType.PYTHON,
        )
        return MessageToDict(job.to_flyte_idl())

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        import pyspark as _pyspark

        ctx = FlyteContextManager.current_context()
        sess_builder = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {user_params.execution_id}")
        if not (ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION):
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
        return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()


# Inject the Spark plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(Spark, PysparkFunctionTask)

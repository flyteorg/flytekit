import os
import sys
from typing import Any, Callable, Dict

from google.protobuf.json_format import MessageToDict

from flytekit.annotated.task import PythonFunctionTask, TaskPlugins
from flytekit.models import task as _task_model
from flytekit.sdk.spark_types import SparkType


class Spark(object):
    def __init__(self, spark_conf: Dict[str, str] = None, hadoop_conf: Dict[str, str] = None):
        self._spark_conf = spark_conf
        self._hadoop_conf = hadoop_conf

    @property
    def spark_conf(self) -> Dict[str, str]:
        return self._spark_conf

    @property
    def hadoop_config(self) -> Dict[str, str]:
        return self._hadoop_conf


class GlobalSparkContext(object):
    """
    TODO revisit this class - it has been copied over from the spark task to avoid cyclic imports
    """

    _SPARK_CONTEXT = None
    _SPARK_SESSION = None

    @classmethod
    def get_spark_context(cls):
        return cls._SPARK_CONTEXT

    @classmethod
    def get_spark_session(cls):
        return cls._SPARK_SESSION

    def __enter__(self):
        import pyspark as _pyspark

        GlobalSparkContext._SPARK_CONTEXT = _pyspark.SparkContext()
        GlobalSparkContext._SPARK_SESSION = _pyspark.sql.SparkSession.builder.appName(
            "Flyte Spark SQL Context"
        ).getOrCreate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        GlobalSparkContext._SPARK_CONTEXT.stop()
        GlobalSparkContext._SPARK_CONTEXT = None
        return False


class PysparkFunctionTask(PythonFunctionTask[Spark]):
    def __init__(
        self, task_config: Spark, task_function: Callable, metadata: _task_model.TaskMetadata, *args, **kwargs
    ):
        super(PysparkFunctionTask, self).__init__(
            task_config=task_config,
            task_type="spark",
            task_function=task_function,
            metadata=metadata,
            ignore_input_vars=["spark_session", "spark_context"],
            *args,
            **kwargs,
        )

    def get_custom(self) -> Dict[str, Any]:
        from flytekit.bin import entrypoint as _entrypoint

        spark_exec_path = os.path.abspath(_entrypoint.__file__)
        if spark_exec_path.endswith(".pyc"):
            spark_exec_path = spark_exec_path[:-1]

        job = _task_model.SparkJob(
            spark_conf=self.task_config.spark_conf,
            hadoop_conf=self.task_config.hadoop_config,
            application_file="local://" + spark_exec_path,
            executor_path=sys.executable,
            main_class="",
            spark_type=SparkType.PYTHON,
        )
        return MessageToDict(job.to_flyte_idl())

    def execute(self, **kwargs) -> Any:
        with GlobalSparkContext() as spark_ctx:
            if "spark_session" in self.native_interface.inputs:
                kwargs["spark_session"] = spark_ctx.get_spark_session()
            if "spark_context" in self.native_interface.inputs:
                kwargs["spark_context"] = spark_ctx.get_spark_context()
            return self._task_function(**kwargs)


TaskPlugins.register_pythontask_plugin(Spark, PysparkFunctionTask)

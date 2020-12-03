import os
import sys
from contextlib import contextmanager
from typing import Any, Callable, Dict

from google.protobuf.json_format import MessageToDict

from flytekit.annotated.context_manager import RegistrationSettings
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.task import TaskPlugins
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
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


@contextmanager
def new_spark_session(name: str):
    import pyspark as _pyspark

    sess = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {name}").getOrCreate()
    yield sess
    # SparkSession.Stop does not work correctly, as it stops the session before all the data is written
    # sess.stop()


class PysparkFunctionTask(PythonFunctionTask[Spark]):
    def __init__(
        self, task_config: Spark, task_function: Callable, metadata: _task_model.TaskMetadata, *args, **kwargs
    ):
        super(PysparkFunctionTask, self).__init__(
            task_config=task_config, task_type="spark", task_function=task_function, metadata=metadata, *args, **kwargs,
        )

    def get_custom(self, settings: RegistrationSettings) -> Dict[str, Any]:
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

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        import pyspark as _pyspark

        sess = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {user_params.execution_id}").getOrCreate()
        b = user_params.builder(user_params)
        b.add_attr("SPARK_SESSION", sess)
        return b.build()


TaskPlugins.register_pythontask_plugin(Spark, PysparkFunctionTask)

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union, cast

import click
from google.protobuf.json_format import MessageToDict

from flytekit import FlyteContextManager, PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec import ImageSpec

from .models import SparkJob, SparkType
from .task import Spark


@dataclass
class Databricks(Spark):
    """
    Use this to configure a Databricks task. Task's marked with this will automatically execute
    natively onto a databricks platform as a distributed execution of spark

    Args:
        databricks_conf: Databricks job configuration compliant with API version 2.1, supporting 2.0 use cases.
        For the configuration structure, visit here.https://docs.databricks.com/dev-tools/api/2.0/jobs.html#request-structure
        For updates in API 2.1, refer to: https://docs.databricks.com/en/workflows/jobs/jobs-api-updates.html
        databricks_instance: Domain name of your deployment. Use the form <account>.cloud.databricks.com.
    """

    databricks_conf: Optional[Dict[str, Union[str, dict]]] = None
    databricks_instance: Optional[str] = None


class DatabricksFunctionTask(AsyncAgentExecutorMixin, PythonFunctionTask[Databricks]):
    """
    Actual Plugin that submits a Databricks job to the Databricks cluster
    """

    _DATABRICKS_TASK_TYPE = "databricks"

    def __init__(
        self,
        task_config: Spark,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        self._default_executor_path: str = task_config.executor_path
        self._default_applications_path: str = task_config.applications_path

        super(DatabricksFunctionTask, self).__init__(
            task_config=task_config,
            task_type=self._DATABRICKS_TASK_TYPE,
            task_function=task_function,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = SparkJob(
            spark_conf=self.task_config.spark_conf,
            hadoop_conf=self.task_config.hadoop_conf,
            application_file=self._default_applications_path or "local://" + settings.entrypoint_settings.path,
            executor_path=self._default_executor_path or settings.python_interpreter,
            main_class="",
            spark_type=SparkType.PYTHON,
        )
        cfg = cast(Databricks, self.task_config)
        job._databricks_conf = cfg.databricks_conf
        job._databricks_instance = cfg.databricks_instance

        return MessageToDict(job.to_flyte_idl())

    def execute(self, **kwargs) -> Any:
        # Use the Databricks agent to run it by default.
        try:
            ctx = FlyteContextManager.current_context()
            if not ctx.file_access.is_remote(ctx.file_access.raw_output_prefix):
                raise ValueError(
                    "To submit a Databricks job locally,"
                    " please set --raw-output-data-prefix to a remote path. e.g. s3://, gcs//, etc."
                )
            if ctx.execution_state and ctx.execution_state.is_local_execution():
                return AsyncAgentExecutorMixin.execute(self, **kwargs)
        except Exception as e:
            click.secho(f"❌ Agent failed to run the task with error: {e}", fg="red")
            click.secho("Falling back to local execution", fg="red")
        return PythonFunctionTask.execute(self, **kwargs)


TaskPlugins.register_pythontask_plugin(Databricks, DatabricksFunctionTask)

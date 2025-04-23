import dataclasses
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union, cast

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
class Spark(object):
    """
    Use this to configure a SparkContext for a your task. Task's marked with this will automatically execute
    natively onto K8s as a distributed execution of spark

    Attributes:
        spark_conf (Optional[Dict[str, str]]): Spark configuration dictionary.
        hadoop_conf (Optional[Dict[str, str]]): Hadoop configuration dictionary.
        executor_path (Optional[str]): Path to the Python binary for PySpark execution.
        applications_path (Optional[str]): Path to the main application file.
        driver_pod (Optional[PodTemplate]): The pod template for the Spark driver pod.
        executor_pod (Optional[PodTemplate]): The pod template for the Spark executor pod.
    """

    spark_conf: Optional[Dict[str, str]] = None
    hadoop_conf: Optional[Dict[str, str]] = None
    executor_path: Optional[str] = None
    applications_path: Optional[str] = None
    driver_pod: Optional[PodTemplate] = None
    executor_pod: Optional[PodTemplate] = None

    def __post_init__(self):
        if self.spark_conf is None:
            self.spark_conf = {}

        if self.hadoop_conf is None:
            self.hadoop_conf = {}


@dataclass
class Databricks(Spark):
    """
    Deprecated. Use DatabricksV2 instead.
    """

    databricks_conf: Optional[Dict[str, Union[str, dict]]] = None
    databricks_instance: Optional[str] = None

    def __post_init__(self):
        logger.warning(
            "Databricks is deprecated. Use 'from flytekitplugins.spark import Databricks' instead,"
            "and make sure to upgrade the version of flyte connector deployment to >v1.13.0.",
        )


@dataclass
class DatabricksV2(Spark):
    """
    Use this to configure a Databricks task. Task's marked with this will automatically execute
    natively onto databricks platform as a distributed execution of spark

    Args:
        databricks_conf: Databricks job configuration compliant with API version 2.1, supporting 2.0 use cases.
        For the configuration structure, visit here.https://docs.databricks.com/dev-tools/api/2.0/jobs.html#request-structure
        For updates in API 2.1, refer to: https://docs.databricks.com/en/workflows/jobs/jobs-api-updates.html
        databricks_instance: Domain name of your deployment. Use the form <account>.cloud.databricks.com.
    """

    databricks_conf: Optional[Dict[str, Union[str, dict]]] = None
    databricks_instance: Optional[str] = None


# This method does not reset the SparkSession since it's a bit hard to handle multiple
# Spark sessions in a single application as it's described in:
# https://stackoverflow.com/questions/41491972/how-can-i-tear-down-a-sparksession-and-create-a-new-one-within-one-application.
def new_spark_session(name: str, conf: Dict[str, str] = None):
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


class PysparkFunctionTask(AsyncConnectorExecutorMixin, PythonFunctionTask[Spark]):
    """
    Actual Plugin that transforms the local python code for execution within a spark context
    """

    _SPARK_TASK_TYPE = "spark"

    def __init__(
        self,
        task_config: Spark,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        self.sess: Optional[SparkSession] = None
        self._default_executor_path: str = task_config.executor_path
        self._default_applications_path: str = task_config.applications_path
        self._container_image = container_image

        if isinstance(container_image, ImageSpec):
            if container_image.base_image is None:
                img = f"cr.flyte.org/flyteorg/flytekit:spark-{DefaultImages.get_version_suffix()}"
                self._container_image = dataclasses.replace(container_image, base_image=img)
                if container_image.builder == DefaultImageBuilder.builder_type:
                    # Install the dependencies in the default venv in the spark base image
                    self._container_image = dataclasses.replace(self._container_image, python_exec="/usr/bin/python3")

                # default executor path and applications path in apache/spark-py:3.3.1
                self._default_executor_path = self._default_executor_path or "/usr/bin/python3"
                self._default_applications_path = (
                    self._default_applications_path or "local:///usr/local/bin/entrypoint.py"
                )

        if isinstance(task_config, DatabricksV2):
            task_type = "databricks"
        else:
            task_type = "spark"

        super(PysparkFunctionTask, self).__init__(
            task_config=task_config,
            task_type=task_type,
            task_function=task_function,
            container_image=self._container_image,
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
            driver_pod=self.to_k8s_pod(self.task_config.driver_pod),
            executor_pod=self.to_k8s_pod(self.task_config.executor_pod),
        )
        if isinstance(self.task_config, (Databricks, DatabricksV2)):
            cfg = cast(DatabricksV2, self.task_config)
            job._databricks_conf = cfg.databricks_conf
            job._databricks_instance = cfg.databricks_instance

        return MessageToDict(job.to_flyte_idl())

    def to_k8s_pod(self, pod_template: Optional[PodTemplate] = None) -> Optional[K8sPod]:
        """
        Convert the podTemplate to K8sPod
        """
        if pod_template is None:
            return None

        task_primary_container_name = (
            self.pod_template.primary_container_name if self.pod_template else PRIMARY_CONTAINER_DEFAULT_NAME
        )

        if pod_template.primary_container_name != task_primary_container_name:
            logger.warning(
                "Primary container name ('%s') set in spark differs from the one in @task ('%s'). "
                "The primary container name in @task will be overridden.",
                pod_template.primary_container_name,
                task_primary_container_name,
            )

        return K8sPod.from_pod_template(pod_template)

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

        if (
            ctx.serialization_settings
            and ctx.serialization_settings.fast_serialization_settings
            and ctx.serialization_settings.fast_serialization_settings.enabled
            and ctx.execution_state
            and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION
        ):
            base_dir = tempfile.mkdtemp()
            file_name = "flyte_wf"
            file_format = "zip"
            shutil.make_archive(f"{base_dir}/{file_name}", file_format, os.getcwd())
            self.sess.sparkContext.addPyFile(f"{base_dir}/{file_name}.{file_format}")

        return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()

    def execute(self, **kwargs) -> Any:
        if isinstance(self.task_config, (Databricks, DatabricksV2)):
            # Use the Databricks connector to run it by default.
            try:
                ctx = FlyteContextManager.current_context()
                if not ctx.file_access.is_remote(ctx.file_access.raw_output_prefix):
                    raise ValueError(
                        "To submit a Databricks job locally,"
                        " please set --raw-output-data-prefix to a remote path. e.g. s3://, gcs//, etc."
                    )
                if ctx.execution_state and ctx.execution_state.is_local_execution():
                    return AsyncConnectorExecutorMixin.execute(self, **kwargs)
            except Exception as e:
                click.secho(f"❌ Connector failed to run the task with error: {e}", fg="red")
                click.secho("Falling back to local execution", fg="red")
        return PythonFunctionTask.execute(self, **kwargs)


# Inject the Spark plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(Spark, PysparkFunctionTask)
TaskPlugins.register_pythontask_plugin(Databricks, PysparkFunctionTask)
TaskPlugins.register_pythontask_plugin(DatabricksV2, PysparkFunctionTask)

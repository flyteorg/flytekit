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
    natively onto databricks platform as a distributed execution of spark.

    Supports both classic compute (clusters) and serverless compute.

    Args:
        databricks_conf: Databricks job configuration compliant with API version 2.1, supporting 2.0 use cases.
            For the configuration structure, visit: https://docs.databricks.com/dev-tools/api/2.0/jobs.html#request-structure
            For updates in API 2.1, refer to: https://docs.databricks.com/en/workflows/jobs/jobs-api-updates.html
        databricks_instance: Domain name of your deployment. Use the form <account>.cloud.databricks.com.

    Compute Modes:
        The connector auto-detects the compute mode based on the databricks_conf contents:

        1. Classic Compute (existing cluster):
            Provide `existing_cluster_id` in databricks_conf.

        2. Classic Compute (new cluster):
            Provide `new_cluster` configuration in databricks_conf.

        3. Serverless Compute (pre-configured environment):
            Provide `environment_key` referencing a pre-configured environment in Databricks.
            Do not include `existing_cluster_id` or `new_cluster`.

        4. Serverless Compute (inline environment spec):
            Provide `environments` array with environment specifications.
            Optionally include `environment_key` to specify which environment to use.
            Do not include `existing_cluster_id` or `new_cluster`.

    Example - Classic Compute with new cluster::

        DatabricksV2(
            databricks_conf={
                "run_name": "my-spark-job",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "m5.xlarge",
                    "num_workers": 2,
                },
            },
            databricks_instance="my-workspace.cloud.databricks.com",
        )

    Example - Serverless Compute with pre-configured environment::

        DatabricksV2(
            databricks_conf={
                "run_name": "my-serverless-job",
                "environment_key": "my-preconfigured-env",
            },
            databricks_instance="my-workspace.cloud.databricks.com",
        )

    Example - Serverless Compute with inline environment spec::

        DatabricksV2(
            databricks_conf={
                "run_name": "my-serverless-job",
                "environment_key": "default",
                "environments": [{
                    "environment_key": "default",
                    "spec": {
                        "client": "1",
                        "dependencies": ["pandas==2.0.0", "numpy==1.24.0"],
                    }
                }],
            },
            databricks_instance="my-workspace.cloud.databricks.com",
        )

    Note:
        Serverless compute has certain limitations compared to classic compute:
        - Only Python and SQL are supported (no Scala or R)
        - Only Spark Connect APIs are supported (no RDD APIs)
        - Must use Unity Catalog for external data sources
        - No support for compute-scoped init scripts or libraries
        For full details, see: https://docs.databricks.com/en/compute/serverless/limitations.html
        
    Serverless Entrypoint:
        Both classic and serverless use the same ``flytetools`` repo for their entrypoints.
        Classic uses ``flytekitplugins/databricks/entrypoint.py`` and serverless uses
        ``flytekitplugins/databricks/entrypoint_serverless.py``. No additional configuration needed.
        
        To override the default, provide ``git_source`` and ``python_file`` in ``databricks_conf``.

    AWS Credentials for Serverless:
        Databricks serverless does not provide AWS credentials via instance metadata.
        To access S3 (for Flyte data), configure a Databricks Service Credential.
        
        The provider name is resolved in this order:
        1. ``databricks_service_credential_provider`` in the task config (per-task override)
        2. ``FLYTE_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER`` environment variable on the connector (default for all tasks)
        
        The entrypoint will use this to obtain AWS credentials via:
        dbutils.credentials.getServiceCredentialsProvider(provider_name)

    Notebook Support:
        To run a Databricks notebook instead of a Python file, set `notebook_path`.
        Parameters can be passed via `notebook_base_parameters`.
        
        Example - Running a notebook::
        
            DatabricksV2(
                databricks_conf={
                    "run_name": "my-notebook-job",
                    "new_cluster": {...},
                },
                databricks_instance="my-workspace.cloud.databricks.com",
                notebook_path="/Users/user@example.com/my-notebook",
                notebook_base_parameters={"param1": "value1"},
            )
    """

    databricks_conf: Optional[Dict[str, Union[str, dict]]] = None
    databricks_instance: Optional[str] = None  # Falls back to FLYTE_DATABRICKS_INSTANCE env var
    databricks_service_credential_provider: Optional[str] = None  # Falls back to FLYTE_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER env var
    notebook_path: Optional[str] = None  # Path to Databricks notebook (e.g., "/Users/user@example.com/notebook")
    notebook_base_parameters: Optional[Dict[str, str]] = None  # Parameters to pass to the notebook


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

        # Serialize to dict
        custom_dict = MessageToDict(job.to_flyte_idl())
        
        # Add DatabricksV2-specific fields (not part of protobuf)
        if isinstance(self.task_config, DatabricksV2):
            cfg = cast(DatabricksV2, self.task_config)
            if cfg.databricks_service_credential_provider:
                custom_dict['databricksServiceCredentialProvider'] = cfg.databricks_service_credential_provider
            if cfg.notebook_path:
                custom_dict['notebookPath'] = cfg.notebook_path
            if cfg.notebook_base_parameters:
                custom_dict['notebookBaseParameters'] = cfg.notebook_base_parameters
        
        return custom_dict

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

    def _is_databricks_serverless(self) -> bool:
        """
        Detect if we're running in Databricks serverless environment.
        
        Serverless uses Spark Connect and requires different SparkSession handling.
        """
        # Check for explicit serverless markers set by our entrypoint
        if os.environ.get("DATABRICKS_SERVERLESS") == "true":
            return True
        if os.environ.get("SPARK_CONNECT_MODE") == "true":
            return True
        
        # Check for Databricks serverless indicators
        # 1. DATABRICKS_RUNTIME_VERSION exists (Databricks environment)
        # 2. No SPARK_HOME (serverless doesn't have traditional Spark)
        is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        
        # Additional check: if using DatabricksV2 with serverless config
        if isinstance(self.task_config, DatabricksV2):
            conf = self.task_config.databricks_conf or {}
            has_serverless_config = (
                "environment_key" in conf or 
                "environments" in conf
            ) and "new_cluster" not in conf and "existing_cluster_id" not in conf
            if has_serverless_config:
                return True
        
        return is_databricks and "SPARK_HOME" not in os.environ

    def _get_databricks_serverless_spark_session(self):
        """
        Get SparkSession in Databricks serverless environment.
        
        The entrypoint injects the SparkSession into:
        1. Custom module '_flyte_spark_session' in sys.modules (most reliable)
        2. builtins.spark (backup)
        
        Returns:
            SparkSession or None if not available
        """
        import sys
        
        # Method 1: Try custom module (most reliable - survives module reloads)
        try:
            if '_flyte_spark_session' in sys.modules:
                spark_module = sys.modules['_flyte_spark_session']
                if hasattr(spark_module, 'spark') and spark_module.spark is not None:
                    logger.info(f"Got SparkSession from _flyte_spark_session module")
                    return spark_module.spark
        except Exception as e:
            logger.debug(f"Could not get spark from _flyte_spark_session: {e}")
        
        # Method 2: Try builtins (backup location)
        try:
            import builtins
            if hasattr(builtins, 'spark') and builtins.spark is not None:
                logger.info(f"Got SparkSession from builtins")
                return builtins.spark
        except Exception as e:
            logger.debug(f"Could not get spark from builtins: {e}")
        
        # Method 3: Try __main__ module
        try:
            import __main__
            if hasattr(__main__, 'spark') and __main__.spark is not None:
                logger.info(f"Got SparkSession from __main__")
                return __main__.spark
        except Exception as e:
            logger.debug(f"Could not get spark from __main__: {e}")
        
        # Method 4: Try active session
        try:
            from pyspark.sql import SparkSession
            active = SparkSession.getActiveSession()
            if active:
                logger.info(f"Got active SparkSession")
                return active
        except Exception as e:
            logger.debug(f"Could not get active SparkSession: {e}")
        
        logger.warning("Could not obtain SparkSession in serverless environment")
        return None

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        import pyspark as _pyspark

        ctx = FlyteContextManager.current_context()
        
        # Databricks serverless uses Spark Connect - SparkSession is pre-configured
        if self._is_databricks_serverless():
            logger.info("Detected Databricks serverless environment - using pre-configured SparkSession")
            self.sess = self._get_databricks_serverless_spark_session()
            
            if self.sess is None:
                logger.warning("No SparkSession available - task will run without Spark")
            
            return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()
        
        # Standard Spark session creation for non-serverless environments
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

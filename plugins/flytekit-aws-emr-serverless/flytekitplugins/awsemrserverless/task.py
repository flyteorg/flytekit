"""
Flyte Task definition for EMR Serverless.

Provides the EMRServerless task configuration dataclass, job driver configs,
and the EMRServerlessTask class for defining EMR Serverless jobs in Flyte
workflows. Supports two execution modes:

1. Script mode: User provides S3 paths to Spark/Hive scripts.
2. Pythonic mode: User writes a @task function and the connector uses the
   Flyte container entrypoint to execute it on EMR Serverless.
"""

import dataclasses
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import FlyteContextManager
from flytekit.extend import TaskPlugins
from flytekit.image_spec import ImageSpec

try:
    from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
except ModuleNotFoundError:
    from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin as AsyncConnectorExecutorMixin

logger = logging.getLogger(__name__)


@dataclass
class EMRServerlessSparkJobDriver:
    """
    Spark job driver configuration for EMR Serverless.

    Attributes:
        entry_point: S3 path to the main application file
            (e.g., ``s3://bucket/scripts/main.py``).
        entry_point_arguments: Arguments to pass to the main application.
        spark_submit_parameters: Spark submit parameters string
            (e.g., ``--conf spark.executor.memory=4g``).
    """

    entry_point: str
    entry_point_arguments: Optional[List[str]] = None
    spark_submit_parameters: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to EMR Serverless API format."""
        result: Dict[str, Any] = {"entryPoint": self.entry_point}
        if self.entry_point_arguments:
            result["entryPointArguments"] = self.entry_point_arguments
        if self.spark_submit_parameters:
            result["sparkSubmitParameters"] = self.spark_submit_parameters
        return result


@dataclass
class EMRServerlessHiveJobDriver:
    """
    Hive job driver configuration for EMR Serverless.

    Attributes:
        query: The Hive query to execute, or S3 path to a query file.
        init_query_file: S3 path to an initialization query file.
        parameters: Parameters string for the query.
    """

    query: str
    init_query_file: Optional[str] = None
    parameters: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to EMR Serverless API format."""
        result: Dict[str, Any] = {"query": self.query}
        if self.init_query_file:
            result["initQueryFile"] = self.init_query_file
        if self.parameters:
            result["parameters"] = self.parameters
        return result


_VALID_ARCHITECTURES = ("X86_64", "ARM64")


@dataclass
class EMRServerless:
    """
    EMR Serverless task configuration.

    Use this to configure an EMR Serverless task. Tasks marked with this
    configuration will execute on AWS EMR Serverless as Spark or Hive jobs.

    Supports two execution modes:

    **Script mode** -- provide a ``spark_job_driver`` or ``hive_job_driver``
    pointing to scripts on S3::

        @task(
            task_config=EMRServerless(
                application_id="00f5abc123def456",
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                spark_job_driver=EMRServerlessSparkJobDriver(
                    entry_point="s3://bucket/scripts/main.py",
                ),
                region="us-east-1",
            )
        )
        def my_spark_job() -> str:
            return "Job submitted"

    **Pythonic mode** -- omit the job driver and write Spark code directly
    in the task function. The connector will use the Flyte container
    entrypoint to execute it on EMR Serverless::

        @task(
            task_config=EMRServerless(
                application_id="00f5abc123def456",
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                region="us-east-1",
            )
        )
        def my_spark_job():
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            spark.range(100).write.parquet("s3://bucket/output")

    Attributes:
        execution_role_arn: IAM role ARN for job execution (required).
        application_id: Existing EMR Serverless application ID.  If not
            provided and ``application_name`` is set, the connector will
            attempt to create a new application (subject to the
            connector-level ``FLYTE_EMR_ALLOW_CREATE_APPLICATION`` policy).
        release_label: EMR release label (default: ``emr-7.0.0``).
        application_type: ``"SPARK"`` or ``"HIVE"`` (default: ``"SPARK"``).
        application_name: Name for a new application.  When
            ``application_id`` is not set and ``application_name`` is
            provided, the connector will create a new application with
            this name (prefixed by the connector's
            ``FLYTE_EMR_APPLICATION_NAME_PREFIX`` if configured).
        sync_image: When ``True`` (the default), the connector will update
            the application's image if the task's ``container_image`` or
            ``image_configuration`` differs from what the app currently has.
        spark_job_driver: Spark job configuration.  When omitted for a SPARK
            application, Pythonic mode is used.
        hive_job_driver: Hive job configuration.
        spark_submit_parameters: Extra ``--conf`` flags to pass to
            ``sparkSubmitParameters`` for *both* script and Pythonic modes.
            In Pythonic mode these are merged with the connector defaults.
        application_configuration: ``applicationConfiguration`` list for
            ``configurationOverrides``.  Merged with any entries already
            in ``configuration_overrides``.
        runtime_configuration: ``runtimeConfiguration`` passed to
            ``CreateApplication`` or ``UpdateApplication``.
        scheduler_configuration: ``SchedulerConfiguration`` for job
            concurrency and queuing (e.g.
            ``{"maxConcurrentRuns": 5, "queueTimeoutMinutes": 30}``).
        auto_stop_config: ``AutoStopConfiguration`` for the application
            (e.g. ``{"enabled": True, "idleTimeoutMinutes": 30}``).
        architecture: Processor architecture for the application.
            ``"X86_64"`` (default) or ``"ARM64"`` (Graviton).
        retry_policy: Job-level retry policy for resiliency (e.g.
            ``{"maxAttempts": 3}``).  Requires EMR 7.1+.
        configuration_overrides: Application and monitoring configuration.
        tags: Resource tags.
        execution_timeout_minutes: Maximum job execution time (default: 60).
        initial_capacity: Pre-initialized worker capacity.
        maximum_capacity: Auto-scaling limits.
        network_configuration: VPC, subnet, and security group settings.
        image_configuration: Custom container image settings.
        region: AWS region (uses boto3 default if not specified).
        inject_flyte_env: When ``True`` (default), the connector appends
            ``--conf spark.emr-serverless.driverEnv.*`` and
            ``--conf spark.executorEnv.*`` entries to ``sparkSubmitParameters``
            so the Spark driver and executors see Flyte runtime context as
            environment variables: ``FLYTE_INTERNAL_EXECUTION_ID``,
            ``FLYTE_INTERNAL_EXECUTION_PROJECT``,
            ``FLYTE_INTERNAL_EXECUTION_DOMAIN``,
            ``FLYTE_INTERNAL_TASK_{PROJECT,DOMAIN,NAME,VERSION}``,
            ``FLYTE_INTERNAL_NODE_ID``, and
            ``FLYTE_INTERNAL_TASK_RETRY_ATTEMPT``.  Any
            ``environment_variables`` set on ``task_execution_metadata``
            (via Flyte ``Environment`` policies) are also forwarded.  Only
            applies to Spark jobs.
    """

    execution_role_arn: str
    application_id: Optional[str] = None
    release_label: str = "emr-7.0.0"
    application_type: str = "SPARK"
    application_name: Optional[str] = None

    sync_image: bool = True
    inject_flyte_env: bool = True

    spark_job_driver: Optional[EMRServerlessSparkJobDriver] = None
    hive_job_driver: Optional[EMRServerlessHiveJobDriver] = None

    spark_submit_parameters: Optional[str] = None
    application_configuration: Optional[List[Dict[str, Any]]] = None
    runtime_configuration: Optional[List[Dict[str, Any]]] = None
    scheduler_configuration: Optional[Dict[str, Any]] = None
    auto_stop_config: Optional[Dict[str, Any]] = None
    architecture: Optional[str] = None
    retry_policy: Optional[Dict[str, Any]] = None

    configuration_overrides: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, str]] = None
    execution_timeout_minutes: int = 60

    initial_capacity: Optional[Dict[str, Any]] = None
    maximum_capacity: Optional[Dict[str, Any]] = None
    network_configuration: Optional[Dict[str, Any]] = None
    image_configuration: Optional[Dict[str, Any]] = None

    region: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.execution_role_arn:
            raise ValueError("execution_role_arn is required")
        if self.application_type not in ("SPARK", "HIVE"):
            raise ValueError(f"application_type must be 'SPARK' or 'HIVE', got '{self.application_type}'")
        if self.application_type == "HIVE" and self.hive_job_driver is None:
            raise ValueError("hive_job_driver is required when application_type is 'HIVE'")
        if self.spark_job_driver and self.hive_job_driver:
            raise ValueError("Only one of spark_job_driver or hive_job_driver can be specified")
        if self.execution_timeout_minutes < 1:
            raise ValueError("execution_timeout_minutes must be at least 1")
        if self.architecture and self.architecture not in _VALID_ARCHITECTURES:
            raise ValueError(f"architecture must be one of {_VALID_ARCHITECTURES}, got '{self.architecture}'")

    @property
    def is_script_mode(self) -> bool:
        """True when user provided an explicit job driver (script mode)."""
        return self.spark_job_driver is not None or self.hive_job_driver is not None

    def get_job_driver(self) -> Dict[str, Any]:
        """Get the job driver dict in EMR Serverless API format."""
        if self.spark_job_driver:
            return {"sparkSubmit": self.spark_job_driver.to_dict()}
        if self.hive_job_driver:
            return {"hive": self.hive_job_driver.to_dict()}
        raise ValueError("No job driver configured -- use Pythonic mode instead")

    def get_effective_configuration_overrides(self) -> Optional[Dict[str, Any]]:
        """Build the merged ``configurationOverrides`` dict.

        Combines ``configuration_overrides`` with ``application_configuration``
        so users can set monitoring config via ``configuration_overrides`` and
        Spark/Hive config via ``application_configuration`` independently.
        """
        if not self.application_configuration and not self.configuration_overrides:
            return None

        result = dict(self.configuration_overrides) if self.configuration_overrides else {}

        if self.application_configuration:
            existing = result.get("applicationConfiguration", [])
            merged = list(existing) + list(self.application_configuration)
            result["applicationConfiguration"] = merged

        return result or None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a dict for ``task_template.custom``."""
        result: Dict[str, Any] = {
            "execution_role_arn": self.execution_role_arn,
            "release_label": self.release_label,
            "application_type": self.application_type,
            "execution_timeout_minutes": self.execution_timeout_minutes,
            "sync_image": self.sync_image,
            "inject_flyte_env": self.inject_flyte_env,
        }
        if self.application_id:
            result["application_id"] = self.application_id
        if self.application_name:
            result["application_name"] = self.application_name
        if self.spark_job_driver:
            result["spark_job_driver"] = self.spark_job_driver.to_dict()
        if self.hive_job_driver:
            result["hive_job_driver"] = self.hive_job_driver.to_dict()
        if self.spark_submit_parameters:
            result["spark_submit_parameters"] = self.spark_submit_parameters
        if self.application_configuration:
            result["application_configuration"] = self.application_configuration
        if self.runtime_configuration:
            result["runtime_configuration"] = self.runtime_configuration
        if self.scheduler_configuration:
            result["scheduler_configuration"] = self.scheduler_configuration
        if self.auto_stop_config:
            result["auto_stop_config"] = self.auto_stop_config
        if self.architecture:
            result["architecture"] = self.architecture
        if self.retry_policy:
            result["retry_policy"] = self.retry_policy
        if self.configuration_overrides:
            result["configuration_overrides"] = self.configuration_overrides
        if self.tags:
            result["tags"] = self.tags
        if self.initial_capacity:
            result["initial_capacity"] = self.initial_capacity
        if self.maximum_capacity:
            result["maximum_capacity"] = self.maximum_capacity
        if self.network_configuration:
            result["network_configuration"] = self.network_configuration
        if self.image_configuration:
            result["image_configuration"] = self.image_configuration
        if self.region:
            result["region"] = self.region
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EMRServerless":
        """Deserialize from a dict (inverse of ``to_dict``)."""
        spark_driver = None
        hive_driver = None

        if "spark_job_driver" in data:
            d = data["spark_job_driver"]
            spark_driver = EMRServerlessSparkJobDriver(
                entry_point=d["entryPoint"],
                entry_point_arguments=d.get("entryPointArguments"),
                spark_submit_parameters=d.get("sparkSubmitParameters"),
            )
        if "hive_job_driver" in data:
            d = data["hive_job_driver"]
            hive_driver = EMRServerlessHiveJobDriver(
                query=d["query"],
                init_query_file=d.get("initQueryFile"),
                parameters=d.get("parameters"),
            )

        return cls(
            execution_role_arn=data["execution_role_arn"],
            application_id=data.get("application_id"),
            release_label=data.get("release_label", "emr-7.0.0"),
            application_type=data.get("application_type", "SPARK"),
            application_name=data.get("application_name"),
            sync_image=data.get("sync_image", True),
            inject_flyte_env=data.get("inject_flyte_env", True),
            spark_job_driver=spark_driver,
            hive_job_driver=hive_driver,
            spark_submit_parameters=data.get("spark_submit_parameters"),
            application_configuration=data.get("application_configuration"),
            runtime_configuration=data.get("runtime_configuration"),
            scheduler_configuration=data.get("scheduler_configuration"),
            auto_stop_config=data.get("auto_stop_config"),
            architecture=data.get("architecture"),
            retry_policy=data.get("retry_policy"),
            configuration_overrides=data.get("configuration_overrides"),
            tags=data.get("tags"),
            execution_timeout_minutes=int(data.get("execution_timeout_minutes", 60)),
            initial_capacity=data.get("initial_capacity"),
            maximum_capacity=data.get("maximum_capacity"),
            network_configuration=data.get("network_configuration"),
            image_configuration=data.get("image_configuration"),
            region=data.get("region"),
        )


EMR_SERVERLESS_BASE_IMAGE = "public.ecr.aws/emr-serverless/spark/emr-7.0.0:latest"


class EMRServerlessTask(AsyncConnectorExecutorMixin, PythonFunctionTask[EMRServerless]):
    """
    EMR Serverless Task implementation.

    Extends ``PythonFunctionTask`` with ``AsyncConnectorExecutorMixin`` to
    enable remote execution via the EMR Serverless connector and local
    testing by mimicking FlytePropeller's connector calls.

    For Pythonic mode the task image must include ``flytekit``.  When an
    ``ImageSpec`` is provided without a ``base_image``, the EMR Serverless
    Spark base image is used automatically (same pattern as
    ``flytekit-spark``).  Users can also pass a plain ECR URI string as
    ``container_image``.
    """

    _TASK_TYPE = "emr_serverless"

    def __init__(
        self,
        task_config: EMRServerless,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs: Any,
    ):
        if isinstance(container_image, ImageSpec) and container_image.base_image is None:
            container_image = dataclasses.replace(container_image, base_image=EMR_SERVERLESS_BASE_IMAGE)

        super().__init__(
            task_config=task_config,
            task_function=task_function,
            task_type=self._TASK_TYPE,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """Serialize task configuration into ``task_template.custom``."""
        return self.task_config.to_dict()

    def execute(self, **kwargs: Any) -> Any:
        """
        Execute the task.

        When running locally (e.g. ``pyflyte run`` without ``--remote``),
        delegate to the connector mixin so it mimics the backend flow.

        When running on an EMR Serverless worker (dispatched by the
        entrypoint), execute the user function directly -- the connector
        already submitted the job; the worker just needs to run the code.
        """
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.is_local_execution():
            return AsyncConnectorExecutorMixin.execute(self, **kwargs)
        return PythonFunctionTask.execute(self, **kwargs)


TaskPlugins.register_pythontask_plugin(EMRServerless, EMRServerlessTask)

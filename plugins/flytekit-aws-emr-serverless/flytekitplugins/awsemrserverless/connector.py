"""
Flyte Async Connector for AWS EMR Serverless.

Handles the complete lifecycle of EMR Serverless jobs:
create (submit), get (poll status), and delete (cancel).
"""

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from flyteidl.core.execution_pb2 import TaskExecution

try:
    from flytekit.extend.backend.base_connector import (
        AsyncConnectorBase,
        ConnectorRegistry,
        Resource,
        ResourceMeta,
    )
except ModuleNotFoundError:
    from flytekit.extend.backend.base_agent import (
        AgentRegistry as ConnectorRegistry,
    )
    from flytekit.extend.backend.base_agent import (
        AsyncAgentBase as AsyncConnectorBase,
    )
    from flytekit.extend.backend.base_agent import (
        Resource,
        ResourceMeta,
    )

from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.core.execution import TaskLog
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from flytekitplugins.awsemrserverless.boto_handler import EMRServerlessHandler
from flytekitplugins.awsemrserverless.task import EMR_SERVERLESS_BASE_IMAGE, EMRServerless

logger = logging.getLogger(__name__)

# Maps EMR Serverless job run states to the canonical flytekit phase strings
# accepted by ``convert_to_flyte_phase`` ("running", "success", "failed").
# This mirrors the upstream flytekit plugin convention (see
# ``plugins/flytekit-aws-sagemaker/flytekitplugins/awssagemaker_inference/connector.py``).
EMR_SERVERLESS_STATES = {
    "PENDING": "Running",
    "SCHEDULED": "Running",
    "SUBMITTED": "Running",
    "RUNNING": "Running",
    "SUCCESS": "Success",
    "FAILED": "Failed",
    "CANCELLING": "Running",
    "CANCELLED": "Failed",
}

FLYTE_TAGS = {"Application": "flyte", "ManagedBy": "flyte-connector"}

_APPLICATION_ID_RE = re.compile(r"^[0-9a-z]+$")

_ENV_ALLOW_CREATE_APPLICATION = "FLYTE_EMR_ALLOW_CREATE_APPLICATION"
_ENV_APPLICATION_NAME_PREFIX = "FLYTE_EMR_APPLICATION_NAME_PREFIX"


@dataclass
class EMRServerlessJobMetadata(ResourceMeta):
    """Metadata persisted by FlytePropeller between connector calls."""

    application_id: str
    job_run_id: str
    region: str
    created_application: bool = False


class EMRServerlessConnector(AsyncConnectorBase):
    """
    Flyte Connector for AWS EMR Serverless.

    Supports two execution modes:

    * **Script mode** -- the user provides an explicit ``spark_job_driver``
      or ``hive_job_driver`` in the task config.  The connector submits the
      job as-is.
    * **Pythonic mode** -- no job driver is provided.  The connector reads
      ``task_template.container`` (image + args) and constructs a
      ``sparkSubmit`` job that runs the Flytekit entrypoint inside the
      user's container image on EMR Serverless.
    """

    name = "EMR Serverless Connector"

    def __init__(self):
        super().__init__(
            task_type_name="emr_serverless",
            metadata_type=EMRServerlessJobMetadata,
        )

    def _get_handler(self, region: Optional[str] = None) -> EMRServerlessHandler:
        return EMRServerlessHandler(region=region)

    @staticmethod
    def _extract_config(task_template: TaskTemplate) -> EMRServerless:
        custom = task_template.custom
        if not custom:
            raise ValueError("Task template has no custom configuration")

        if hasattr(custom, "fields"):
            from google.protobuf.json_format import MessageToDict

            config_dict = MessageToDict(custom)
        else:
            config_dict = dict(custom)

        config = EMRServerless.from_dict(config_dict)
        logger.debug(
            "Extracted task config: application_id=%s, application_name=%s, "
            "application_type=%s, region=%s, script_mode=%s, sync_image=%s",
            config.application_id,
            config.application_name,
            config.application_type,
            config.region,
            config.is_script_mode,
            config.sync_image,
        )
        return config

    # The Pythonic-mode entrypoint script that EMR workers execute as
    # ``sparkSubmit.entryPoint``.  Lives in its own module so that:
    #   * it is reviewable / blame-able in version control,
    #   * it can be unit-tested as Python (see ``tests/test_entrypoint.py``),
    #   * the connector ships it via the same wheel it ships in,
    #     mirroring the upstream Databricks pattern of treating the
    #     entrypoint as a first-class plugin asset.
    _ENTRYPOINT_PATH: Path = (Path(__file__).parent / "_entrypoint.py").resolve()

    @classmethod
    def _read_entrypoint_bytes(cls) -> bytes:
        """Read the entrypoint file as bytes.  Cached on the class.

        Reading from disk (instead of importing the module and using
        ``inspect.getsource``) keeps the byte-stream byte-for-byte
        identical to what EMR will execute, which is the input to the
        content hash.
        """
        cached = getattr(cls, "_entrypoint_bytes_cache", None)
        if cached is None:
            cached = cls._ENTRYPOINT_PATH.read_bytes()
            cls._entrypoint_bytes_cache = cached
        return cached

    @staticmethod
    def _resolve_entrypoint_bucket(config: EMRServerless) -> str:
        """Determine the S3 bucket for the entrypoint script.

        Resolution order:
        1. FLYTE_EMR_ENTRYPOINT_S3_BUCKET env var
        2. The S3 monitoring log URI from configuration_overrides (reuses the same bucket)
        3. FLYTE_AWS_S3_BUCKET env var (common Flyte storage bucket)
        """
        bucket = os.environ.get("FLYTE_EMR_ENTRYPOINT_S3_BUCKET")
        if bucket:
            resolved = bucket.removeprefix("s3://").split("/")[0]
            logger.debug("Resolved entrypoint bucket from FLYTE_EMR_ENTRYPOINT_S3_BUCKET: %s", resolved)
            return resolved

        if config.configuration_overrides:
            log_uri = (
                config.configuration_overrides.get("monitoringConfiguration", {})
                .get("s3MonitoringConfiguration", {})
                .get("logUri", "")
            )
            if log_uri.startswith("s3://"):
                resolved = log_uri.removeprefix("s3://").split("/")[0]
                logger.debug("Resolved entrypoint bucket from monitoringConfiguration logUri: %s", resolved)
                return resolved

        bucket = os.environ.get("FLYTE_AWS_S3_BUCKET")
        if bucket:
            resolved = bucket.removeprefix("s3://").split("/")[0]
            logger.debug("Resolved entrypoint bucket from FLYTE_AWS_S3_BUCKET: %s", resolved)
            return resolved

        raise ValueError(
            "Pythonic mode needs an S3 bucket to upload the Flytekit entrypoint. "
            "Set FLYTE_EMR_ENTRYPOINT_S3_BUCKET env var, or add "
            "monitoringConfiguration.s3MonitoringConfiguration.logUri to "
            "configuration_overrides in your task config."
        )

    def _ensure_entrypoint_on_s3(self, handler: EMRServerlessHandler, config: EMRServerless) -> str:
        """Upload the Flytekit entrypoint to S3 if not present, return the s3:// URI.

        The S3 key is content-addressed (``entrypoint-<sha256[:12]>.py``)
        so each plugin version produces a unique, immutable artifact.
        Old hashes remain in the bucket indefinitely so in-flight EMR
        jobs that captured an older URI in their job spec keep working
        across connector upgrades.
        """
        content = self._read_entrypoint_bytes()
        content_hash = hashlib.sha256(content).hexdigest()[:12]

        bucket = self._resolve_entrypoint_bucket(config)

        key = f"flyte/emr-serverless/entrypoint-{content_hash}.py"
        s3_uri = f"s3://{bucket}/{key}"

        import boto3

        s3 = boto3.client("s3", region_name=handler.region)
        try:
            s3.head_object(Bucket=bucket, Key=key)
            logger.debug("Entrypoint already exists at %s", s3_uri)
        except Exception:
            logger.info("Uploading Flytekit entrypoint to %s", s3_uri)
            s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType="text/x-python")

        return s3_uri

    _PYTHONIC_SPARK_DEFAULTS = (
        "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3 "
        "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/usr/bin/python3 "
        "--conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3"
    )

    # Characters that would break ``--conf k=v`` tokenisation if embedded in
    # an env value.  EMR Serverless does not support quoting values here,
    # so we drop the variable rather than risk a malformed sparkSubmit line.
    _UNSAFE_ENV_VALUE_CHARS = re.compile(r"[\s='\"]")

    @staticmethod
    def _build_flyte_env_vars(
        task_execution_metadata: Optional[Any],
    ) -> Dict[str, str]:
        """Return a dict of FLYTE_INTERNAL_* env vars derived from the
        task execution metadata, plus any user-supplied env vars from
        ``task_execution_metadata.environment_variables``.

        Returns an empty dict when ``task_execution_metadata`` is ``None``
        (e.g. local / unit-test execution).
        """
        if task_execution_metadata is None:
            return {}

        env: Dict[str, str] = {}

        task_exec_id = getattr(task_execution_metadata, "task_execution_id", None)
        if task_exec_id is None:
            return env

        task_id = getattr(task_exec_id, "task_id", None)
        if task_id is not None:
            env["FLYTE_INTERNAL_TASK_PROJECT"] = getattr(task_id, "project", "") or ""
            env["FLYTE_INTERNAL_TASK_DOMAIN"] = getattr(task_id, "domain", "") or ""
            env["FLYTE_INTERNAL_TASK_NAME"] = getattr(task_id, "name", "") or ""
            env["FLYTE_INTERNAL_TASK_VERSION"] = getattr(task_id, "version", "") or ""

        node_exec_id = getattr(task_exec_id, "node_execution_id", None)
        if node_exec_id is not None:
            env["FLYTE_INTERNAL_NODE_ID"] = getattr(node_exec_id, "node_id", "") or ""
            wf_exec_id = getattr(node_exec_id, "execution_id", None)
            if wf_exec_id is not None:
                env["FLYTE_INTERNAL_EXECUTION_ID"] = getattr(wf_exec_id, "name", "") or ""
                env["FLYTE_INTERNAL_EXECUTION_PROJECT"] = getattr(wf_exec_id, "project", "") or ""
                env["FLYTE_INTERNAL_EXECUTION_DOMAIN"] = getattr(wf_exec_id, "domain", "") or ""

        retry = getattr(task_exec_id, "retry_attempt", None)
        if retry is not None:
            env["FLYTE_INTERNAL_TASK_RETRY_ATTEMPT"] = str(retry)

        user_env = getattr(task_execution_metadata, "environment_variables", None) or {}
        for k, v in user_env.items():
            if k and v is not None:
                env[str(k)] = str(v)

        return {k: v for k, v in env.items() if v != ""}

    @classmethod
    def _format_env_as_spark_conf(cls, env_vars: Dict[str, str]) -> str:
        """Convert a dict of env vars into Spark driver+executor ``--conf`` flags.

        Produces entries of the form::

            --conf spark.emr-serverless.driverEnv.KEY=VALUE
            --conf spark.executorEnv.KEY=VALUE

        Values containing whitespace or quote characters are skipped with
        a warning (EMR Serverless does not support escaping here).
        """
        if not env_vars:
            return ""

        parts = []
        for key, value in env_vars.items():
            if cls._UNSAFE_ENV_VALUE_CHARS.search(value):
                logger.warning(
                    "Skipping env var %s: value contains characters that would "
                    "break sparkSubmitParameters tokenisation",
                    key,
                )
                continue
            parts.append(f"--conf spark.emr-serverless.driverEnv.{key}={value}")
            parts.append(f"--conf spark.executorEnv.{key}={value}")
        return " ".join(parts)

    @classmethod
    def _append_flyte_env_to_spark_params(
        cls,
        existing: Optional[str],
        task_execution_metadata: Optional[Any],
        *,
        enabled: bool,
    ) -> Optional[str]:
        """Append Flyte context env vars to an existing sparkSubmitParameters.

        Returns ``existing`` unchanged when ``enabled`` is False, when no
        metadata is available, or when there are no env vars to inject.
        """
        if not enabled:
            return existing
        env_vars = cls._build_flyte_env_vars(task_execution_metadata)
        if not env_vars:
            return existing
        conf = cls._format_env_as_spark_conf(env_vars)
        if not conf:
            return existing
        logger.info("Injecting %d Flyte env var(s) into sparkSubmitParameters", len(env_vars))
        logger.debug("Injected Flyte env vars: %s", sorted(env_vars.keys()))
        return f"{existing} {conf}".strip() if existing else conf

    def _build_pythonic_job_driver(
        self, task_template: TaskTemplate, config: EMRServerless, handler: EMRServerlessHandler
    ) -> Dict[str, Any]:
        """
        Build a sparkSubmit job driver from the Flyte container entrypoint.

        The Databricks connector fetches its entrypoint from a Git repo.
        EMR Serverless doesn't support Git sources, so we upload the
        Flytekit entrypoint script to S3 and reference it from there.
        ``container.args`` are passed as entrypoint arguments so that
        the task function is executed inside the EMR Spark runtime.

        Pythonic mode requires a custom Docker image on the EMR Serverless
        application that includes flytekit and its dependencies.  The driver
        and executor Python paths are set explicitly so the container's
        Python (which has flytekit) is used instead of the EMR default.
        """
        container = task_template.container
        if container is None:
            raise ValueError(
                "Pythonic mode requires a container image. Either provide a "
                "spark_job_driver for script mode, or ensure the task has a container image."
            )

        logger.info(
            "Building Pythonic mode job driver: container_image=%s, args_count=%d",
            container.image,
            len(container.args) if container.args else 0,
        )
        logger.debug("Pythonic mode container.args: %s", list(container.args) if container.args else [])

        entrypoint_s3_uri = self._ensure_entrypoint_on_s3(handler, config)
        logger.info("Entrypoint S3 URI: %s", entrypoint_s3_uri)

        entry_point_args = list(container.args) if container.args else []

        user_params = config.spark_submit_parameters
        spark_submit_params = self._merge_spark_submit_params(user_params)
        if user_params:
            logger.info("Merged user spark_submit_parameters with Pythonic defaults")
            logger.debug("Final sparkSubmitParameters: %s", spark_submit_params)

        spark_submit: Dict[str, Any] = {
            "entryPoint": entrypoint_s3_uri,
            "entryPointArguments": entry_point_args,
        }
        if spark_submit_params:
            spark_submit["sparkSubmitParameters"] = spark_submit_params

        return {"sparkSubmit": spark_submit}

    def _merge_spark_submit_params(self, user_params: Optional[str]) -> str:
        """Merge user-provided spark submit params with Pythonic mode defaults.

        User-provided values take precedence -- if the user already sets
        e.g. ``spark.executorEnv.PYSPARK_PYTHON``, the default is skipped.
        """
        defaults = self._PYTHONIC_SPARK_DEFAULTS
        if not user_params:
            return defaults

        merged_parts = []
        for token in defaults.split("--conf "):
            token = token.strip()
            if not token:
                continue
            key = token.split("=", 1)[0]
            if key not in user_params:
                merged_parts.append(f"--conf {token}")

        return f"{user_params} {' '.join(merged_parts)}".strip()

    @staticmethod
    def _resolve_image_configuration(
        task_template: TaskTemplate,
        config: EMRServerless,
        *,
        for_create: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Derive EMR Serverless ``imageConfiguration`` following community patterns.

        Resolution order:
        1. Explicit ``image_configuration`` in the task config -- always wins.
        2. ``container_image`` on the task (``task_template.container.image``).
        3. For script mode, ``None`` is fine (default EMR image works).
        4. For Pythonic mode when creating a new app, fall back to the base image.
        5. For Pythonic mode on an existing app, raise with a clear message
           (the app should already have a custom image).
        """
        logger.debug("Resolving image configuration: for_create=%s, script_mode=%s", for_create, config.is_script_mode)

        if config.image_configuration:
            logger.info("Using explicit image_configuration from task config: %s", config.image_configuration)
            return config.image_configuration

        container = task_template.container
        if container and getattr(container, "image", None):
            logger.info(
                "Deriving imageConfiguration from container_image: %s",
                container.image,
            )
            return {"imageUri": container.image}

        if config.is_script_mode:
            logger.debug("Script mode with no custom image; using default EMR image")
            return None

        if for_create:
            logger.info(
                "No image specified for new application; using default base image: %s",
                EMR_SERVERLESS_BASE_IMAGE,
            )
            return {"imageUri": EMR_SERVERLESS_BASE_IMAGE}

        raise ValueError(
            "Pythonic mode requires a custom Docker image with flytekit installed "
            "on the EMR Serverless workers. Provide a container_image on the task:\n\n"
            '  @task(task_config=EMRServerless(...), container_image="<ECR_URI>")\n\n'
            "Or use an existing application_id whose application already has a "
            "custom image configured."
        )

    def _merge_tags(self, user_tags: Optional[Dict[str, str]]) -> Dict[str, str]:
        tags = dict(FLYTE_TAGS)
        if user_tags:
            tags.update(user_tags)
        return tags

    @staticmethod
    def _get_container_image(task_template: TaskTemplate) -> Optional[str]:
        """Extract the container image URI from the task template."""
        container = task_template.container
        if container and getattr(container, "image", None):
            return container.image
        return None

    @staticmethod
    def _is_create_application_allowed() -> bool:
        """Check the connector-level policy for application creation.

        Reads ``FLYTE_EMR_ALLOW_CREATE_APPLICATION`` (default ``"false"``).
        Only ``"true"`` (case-insensitive) permits dynamic creation.
        """
        raw_value = os.environ.get(_ENV_ALLOW_CREATE_APPLICATION, "false")
        allowed = raw_value.strip().lower() == "true"
        logger.debug(
            "Application creation policy check: %s=%r, allowed=%s",
            _ENV_ALLOW_CREATE_APPLICATION,
            raw_value,
            allowed,
        )
        return allowed

    @staticmethod
    def _apply_application_name_prefix(name: str) -> str:
        """Prepend the connector-level prefix to the application name.

        Reads ``FLYTE_EMR_APPLICATION_NAME_PREFIX``.  When set (e.g.
        ``flyte-prod-``), the final name becomes ``flyte-prod-my-etl-app``.
        """
        prefix = os.environ.get(_ENV_APPLICATION_NAME_PREFIX, "").strip()
        if prefix and not name.startswith(prefix):
            prefixed = f"{prefix}{name}"
            logger.info("Applied application name prefix: '%s' -> '%s'", name, prefixed)
            return prefixed
        if prefix:
            logger.debug("Application name '%s' already starts with prefix '%s', skipping", name, prefix)
        return name

    async def _sync_application_image(
        self,
        handler: EMRServerlessHandler,
        application_id: str,
        task_template: TaskTemplate,
        config: EMRServerless,
    ) -> None:
        """Update the application image if ``container_image`` differs from the app.

        The desired image is derived from (in order):
        1. ``task_template.container.image`` (the ``container_image`` decorator arg)
        2. ``config.image_configuration`` (explicit ``image_configuration`` in task_config)

        If neither is set, this is a no-op (script mode with default EMR image).
        """
        logger.debug("Image sync: checking application %s", application_id)

        if config.is_script_mode and not config.image_configuration:
            logger.debug(
                "Image sync: script mode with no explicit image_configuration, skipping for application %s",
                application_id,
            )
            return

        desired_uri = self._get_container_image(task_template)
        if not desired_uri and config.image_configuration:
            desired_uri = config.image_configuration.get("imageUri", "")
            logger.debug("Image sync: using image_configuration as fallback: %s", desired_uri)
        if not desired_uri:
            logger.debug("Image sync: no desired image URI found, skipping for application %s", application_id)
            return

        logger.debug("Image sync: desired image for application %s: %s", application_id, desired_uri)
        app = await handler.get_application(application_id)
        current_image = app.get("imageConfiguration", {}).get("imageUri", "")
        logger.debug("Image sync: current image on application %s: %s", application_id, current_image or "(none)")

        if current_image == desired_uri:
            logger.info(
                "Image sync: application %s already has image %s, no update needed", application_id, desired_uri
            )
            return

        logger.info(
            "Image sync: updating application %s image: %s -> %s",
            application_id,
            current_image or "(none)",
            desired_uri,
        )
        await handler.update_application(
            application_id=application_id,
            image_configuration={"imageUri": desired_uri},
        )
        logger.info("Image sync: successfully updated application %s image", application_id)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        task_execution_metadata: Optional[Any] = None,
        **kwargs: Any,
    ) -> EMRServerlessJobMetadata:
        task_name = task_template.id.name if task_template.id else "(unknown)"
        logger.info("create() called for task: %s", task_name)

        if task_execution_metadata is None:
            task_execution_metadata = kwargs.get("task_execution_metadata")

        config = self._extract_config(task_template)
        handler = self._get_handler(config.region)

        application_id = config.application_id
        created_application = False

        # --- Resolve application by name if the ID looks like a name ---
        if application_id and not _APPLICATION_ID_RE.match(application_id):
            app_name = application_id
            logger.info(
                "application_id '%s' does not look like an AWS ID, treating as application name",
                app_name,
            )
            resolved_id = await handler.find_application_by_name(app_name)
            if resolved_id:
                logger.info("Resolved application name '%s' to ID: %s", app_name, resolved_id)
                application_id = resolved_id
            else:
                raise ValueError(
                    f"No active EMR Serverless application found with name '{app_name}'. "
                    f"application_id must be a valid AWS application ID (e.g. '00fm0lpr3kbcq60p') "
                    f"or the name of an existing application."
                )

        # --- Create a new application if needed ---
        if not application_id:
            logger.info("No application_id provided, checking if dynamic creation is possible")

            if not config.application_name:
                logger.error("No application_id and no application_name specified, cannot proceed")
                raise ValueError(
                    "No application_id provided and no application_name specified. "
                    "Either set application_id to an existing EMR Serverless application, "
                    "or set application_name to create a new one "
                    f"(requires {_ENV_ALLOW_CREATE_APPLICATION}=true on the connector)."
                )

            if not self._is_create_application_allowed():
                logger.warning(
                    "Dynamic application creation requested for '%s' but blocked by connector policy",
                    config.application_name,
                )
                raise ValueError(
                    "Dynamic application creation is disabled by the connector "
                    f"({_ENV_ALLOW_CREATE_APPLICATION} is not 'true'). "
                    "Contact your platform team to enable it, or set application_id "
                    "to an existing EMR Serverless application."
                )

            app_name = self._apply_application_name_prefix(config.application_name)

            image_config = self._resolve_image_configuration(task_template, config, for_create=True)

            logger.info(
                "Creating new EMR Serverless application: name=%s, release_label=%s, type=%s, architecture=%s",
                app_name,
                config.release_label,
                config.application_type,
                config.architecture,
            )
            application_id = await handler.create_application(
                name=app_name,
                release_label=config.release_label,
                application_type=config.application_type,
                initial_capacity=config.initial_capacity,
                maximum_capacity=config.maximum_capacity,
                network_configuration=config.network_configuration,
                image_configuration=image_config,
                tags=self._merge_tags(config.tags),
                architecture=config.architecture,
                runtime_configuration=config.runtime_configuration,
                scheduler_configuration=config.scheduler_configuration,
                auto_stop_config=config.auto_stop_config,
            )
            created_application = True
            logger.info("Created application %s successfully", application_id)
        else:
            logger.info("Using existing application: %s", application_id)

        # --- Sync image on existing apps if sync_image is enabled ---
        if not created_application and config.sync_image:
            logger.info("sync_image is enabled, checking application image for %s", application_id)
            await self._sync_application_image(handler, application_id, task_template, config)
        elif not created_application:
            logger.debug("sync_image is disabled, skipping image sync for %s", application_id)

        logger.info("Ensuring application %s is in STARTED state", application_id)
        await handler.ensure_application_started(application_id)

        # --- Build job driver ---
        if config.is_script_mode:
            logger.info("Building job driver in script mode")
            job_driver = config.get_job_driver()
            if config.spark_submit_parameters and "sparkSubmit" in job_driver:
                existing = job_driver["sparkSubmit"].get("sparkSubmitParameters", "")
                merged = f"{existing} {config.spark_submit_parameters}".strip()
                job_driver["sparkSubmit"]["sparkSubmitParameters"] = merged
                logger.info("Merged top-level spark_submit_parameters into script mode job driver")
                logger.debug("Final sparkSubmitParameters: %s", merged)
            if "sparkSubmit" in job_driver:
                current = job_driver["sparkSubmit"].get("sparkSubmitParameters")
                updated = self._append_flyte_env_to_spark_params(
                    current,
                    task_execution_metadata,
                    enabled=config.inject_flyte_env,
                )
                if updated != current:
                    job_driver["sparkSubmit"]["sparkSubmitParameters"] = updated
            elif config.inject_flyte_env:
                logger.debug(
                    "Skipping Flyte env injection: non-Spark job driver (%s)",
                    next(iter(job_driver.keys()), "unknown"),
                )
        else:
            logger.info("Building job driver in Pythonic mode")
            job_driver = self._build_pythonic_job_driver(task_template, config, handler)
            if "sparkSubmit" in job_driver:
                current = job_driver["sparkSubmit"].get("sparkSubmitParameters")
                updated = self._append_flyte_env_to_spark_params(
                    current,
                    task_execution_metadata,
                    enabled=config.inject_flyte_env,
                )
                if updated != current:
                    job_driver["sparkSubmit"]["sparkSubmitParameters"] = updated

        job_name = task_template.id.name if task_template.id else None

        effective_config_overrides = config.get_effective_configuration_overrides()
        if effective_config_overrides:
            logger.debug(
                "Effective configuration overrides keys: %s",
                list(effective_config_overrides.keys()),
            )

        logger.info(
            "Submitting job run: application=%s, job_name=%s, execution_role=%s, timeout=%dm",
            application_id,
            job_name,
            config.execution_role_arn,
            config.execution_timeout_minutes,
        )
        job_run_id = await handler.start_job_run(
            application_id=application_id,
            execution_role_arn=config.execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=effective_config_overrides,
            tags=self._merge_tags(config.tags),
            execution_timeout_minutes=config.execution_timeout_minutes,
            name=job_name,
            retry_policy=config.retry_policy,
        )

        region = config.region or handler.client.meta.region_name
        logger.info(
            "Job submitted successfully: application=%s, job_run_id=%s, region=%s, created_application=%s",
            application_id,
            job_run_id,
            region,
            created_application,
        )

        return EMRServerlessJobMetadata(
            application_id=application_id,
            job_run_id=job_run_id,
            region=region,
            created_application=created_application,
        )

    async def get(
        self,
        resource_meta: EMRServerlessJobMetadata,
        **kwargs: Any,
    ) -> Resource:
        logger.debug(
            "get() called: application=%s, job_run_id=%s, region=%s",
            resource_meta.application_id,
            resource_meta.job_run_id,
            resource_meta.region,
        )
        handler = self._get_handler(resource_meta.region)

        try:
            job = await handler.get_job_run(
                application_id=resource_meta.application_id,
                job_run_id=resource_meta.job_run_id,
            )
        except Exception as e:
            logger.warning(
                "Failed to retrieve job %s on application %s: %s",
                resource_meta.job_run_id,
                resource_meta.application_id,
                e,
            )
            return Resource(
                phase=TaskExecution.FAILED,
                message=f"Job not found: {resource_meta.job_run_id}",
            )

        state = job.get("state", "UNKNOWN")
        state_details = job.get("stateDetails", "")
        phase = convert_to_flyte_phase(EMR_SERVERLESS_STATES.get(state, "Running"))

        message = f"EMR Serverless job state: {state}"
        if state_details:
            message = f"{message} - {state_details}"

        logger.info(
            "Job %s status: state=%s, phase=%s",
            resource_meta.job_run_id,
            state,
            phase,
        )

        log_links = self._get_log_links(resource_meta)

        return Resource(phase=phase, message=message, log_links=log_links)

    def _get_log_links(self, resource_meta: EMRServerlessJobMetadata) -> list:
        region = resource_meta.region or "us-east-1"
        console_url = (
            f"https://{region}.console.aws.amazon.com/emr/home?region={region}"
            f"#/serverless/{resource_meta.application_id}/jobs/{resource_meta.job_run_id}"
        )
        return [TaskLog(uri=console_url, name="EMR Serverless Console").to_flyte_idl()]

    async def delete(
        self,
        resource_meta: EMRServerlessJobMetadata,
        **kwargs: Any,
    ) -> None:
        logger.info(
            "delete() called: application=%s, job_run_id=%s, region=%s",
            resource_meta.application_id,
            resource_meta.job_run_id,
            resource_meta.region,
        )
        handler = self._get_handler(resource_meta.region)
        try:
            await handler.cancel_job_run(
                application_id=resource_meta.application_id,
                job_run_id=resource_meta.job_run_id,
            )
            logger.info("Delete completed for job %s", resource_meta.job_run_id)
        except Exception as e:
            logger.warning(
                "Failed to cancel job %s on application %s: %s",
                resource_meta.job_run_id,
                resource_meta.application_id,
                e,
            )


ConnectorRegistry.register(EMRServerlessConnector())

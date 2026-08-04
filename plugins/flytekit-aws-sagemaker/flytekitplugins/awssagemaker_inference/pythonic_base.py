"""Shared foundation for SageMaker "Pythonic mode" job tasks and connectors.

Pythonic mode lets users run a Flyte ``@task`` function body directly inside a
SageMaker job container (Processing or Training) instead of supplying a boto3
config + prebuilt algorithm image. It mirrors ``flytekit-aws-emr-serverless``
but is simpler: SageMaker runs the container directly via the job's
``ContainerEntrypoint``, so there is no S3 entrypoint shim to upload — the
connector points ``ContainerEntrypoint`` at the rendered Flyte command
(``task_template.container.args``) on the user's flytekit-containing image.

The mechanics are identical for Processing and Training; the only per-service
differences are captured as overridable class attributes / methods on
``PythonicSageMakerJobConnector``:

* ``create_method`` / ``describe_method`` / ``stop_method`` — boto3 method names
* ``job_name_key`` / ``status_key`` / ``secondary_status_key`` — response fields
* ``_state_map`` — service status -> Flyte phase
* ``_build_request`` — assemble the ``create_*_job`` request (``AppSpecification``
  vs ``AlgorithmSpecification``, ``ProcessingResources`` vs ``ResourceConfig``)

Outputs flow back the Flyte-native way: the inner ``pyflyte-execute`` writes
``outputs.pb`` to the rendered ``--output-prefix`` and the connector returns
``outputs=None`` so flytekit materializes the typed return. Because SageMaker
only surfaces a job status (and ``pyflyte-execute`` exits 0 even on user error),
the connector checks for an ``error.pb`` at the output prefix on ``Completed`` to
avoid reporting a false success — the SageMaker-native equivalent of EMR's
exit-code translation shim.
"""

import dataclasses
import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import cloudpickle
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import FlyteContext, ImageSpec, PythonFunctionTask
from flytekit.configuration import DefaultImages, SerializationSettings
from flytekit.core.constants import FLYTE_FAIL_ON_ERROR
from flytekit.core.context_manager import FlyteContextManager
from flytekit.extend.backend.base_connector import (
    AsyncConnectorBase,
    AsyncConnectorExecutorMixin,
    Resource,
    ResourceMeta,
)
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from .boto3_mixin import Boto3ConnectorMixin, CustomException

# Default base image for Pythonic-mode tasks. Applied (EMR-style) when the user
# passes an ``ImageSpec`` without an explicit ``base_image`` so the built image
# is guaranteed to contain flytekit, which the in-container ``pyflyte-execute``
# needs. Users can always override by setting ``base_image`` or passing a plain
# ECR image URI string as ``container_image``.
SAGEMAKER_PYTHONIC_BASE_IMAGE = DefaultImages.default_image()

# SageMaker job names: <= 63 chars, must match ^[a-zA-Z0-9](-*[a-zA-Z0-9])*.
_MAX_JOB_NAME_LEN = 63
_INVALID_JOB_NAME_CHARS = re.compile(r"[^a-zA-Z0-9-]")


@dataclass
class PythonicJobConfig:
    """Shared task configuration for SageMaker Pythonic-mode jobs.

    :param execution_role_arn: IAM role SageMaker assumes to run the job (S3/ECR/logs).
    :param region: AWS region for the SageMaker client.
    :param instance_type: SageMaker ML instance type for the job.
    :param instance_count: Number of instances.
    :param volume_size_in_gb: Attached EBS volume size.
    :param max_runtime_in_seconds: Hard stop for the job.
    :param environment: Extra environment variables for the container.
    :param kms_key_id: Optional KMS key for the attached storage volume.
    :param tags: Resource tags (dict; converted to the boto3 ``[{Key, Value}]`` shape).
    :param job_name_prefix: Prefix for the generated SageMaker job name.
    """

    execution_role_arn: str
    region: str
    instance_type: str = "ml.m5.large"
    instance_count: int = 1
    volume_size_in_gb: int = 30
    max_runtime_in_seconds: int = 3600
    environment: Optional[Dict[str, str]] = None
    kms_key_id: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    job_name_prefix: str = "flyte-"

    def __post_init__(self) -> None:
        if not self.execution_role_arn:
            raise ValueError("execution_role_arn is required")
        if not self.region:
            raise ValueError("region is required")
        if self.instance_count != 1:
            raise ValueError("Pythonic SageMaker jobs currently require instance_count=1")
        if self.volume_size_in_gb < 1:
            raise ValueError("volume_size_in_gb must be at least 1")
        if self.max_runtime_in_seconds < 1:
            raise ValueError("max_runtime_in_seconds must be at least 1")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict for ``task_template.custom`` (drops Nones)."""
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PythonicJobConfig":
        """Deserialize from a dict (inverse of ``to_dict``)."""
        field_names = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in field_names})


class PythonicSageMakerJobTask(AsyncConnectorExecutorMixin, PythonFunctionTask):
    """Base class for SageMaker Pythonic-mode tasks (Processing / Training).

    Subclasses only set ``_TASK_TYPE``. This base applies the default base image
    to bare ``ImageSpec``s, serializes the task config into ``custom``, and
    dispatches ``execute()`` between local-mimic (connector) and in-container
    (run the user function) — the same shape as ``EMRServerlessTask``.
    """

    _TASK_TYPE = "pythonic-sagemaker-job"

    def __init__(self, task_config, task_function, container_image=None, **kwargs):
        if container_image is None:
            raise ValueError(
                "Pythonic SageMaker jobs require an explicit container_image that can be pushed to or resolved from ECR."
            )
        if isinstance(container_image, ImageSpec) and container_image.base_image is None:
            container_image = dataclasses.replace(container_image, base_image=SAGEMAKER_PYTHONIC_BASE_IMAGE)

        super().__init__(
            task_config=task_config,
            task_function=task_function,
            task_type=self._TASK_TYPE,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return self.task_config.to_dict()

    def execute(self, **kwargs: Any) -> Any:
        """Local ``pyflyte run`` mimics the backend via the connector; on the
        SageMaker worker (dispatched by the rendered entrypoint) run the user
        function directly."""
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.is_local_execution():
            return AsyncConnectorExecutorMixin.execute(self, **kwargs)
        return PythonFunctionTask.execute(self, **kwargs)


@dataclass
class PythonicJobMetadata(ResourceMeta):
    """Metadata persisted by FlytePropeller between connector calls.

    ``output_prefix`` is retained so ``get()`` can check for ``error.pb`` written
    by the in-container ``pyflyte-execute``.
    """

    job_name: str
    output_prefix: str
    region: Optional[str] = None
    has_outputs: bool = False

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "PythonicJobMetadata":
        return cloudpickle.loads(data)


def _make_job_name(
    prefix: str,
    task_execution_metadata: Optional[Any],
    task_template: TaskTemplate,
    output_prefix: str = "",
) -> str:
    """Build a deterministic, unique, SageMaker-valid (<=63 char) job name.

    Derived from the Flyte execution + node + retry so retries get fresh names
    while a connector re-invocation for the same attempt reuses the same name
    (the create() idempotency handler then treats it as already-running).
    """
    seed_parts = [output_prefix]
    teid = getattr(task_execution_metadata, "task_execution_id", None) if task_execution_metadata else None
    if teid is not None:
        node = getattr(teid, "node_execution_id", None)
        if node is not None:
            ex = getattr(node, "execution_id", None)
            if ex is not None:
                seed_parts.extend(
                    [
                        getattr(ex, "project", "") or "",
                        getattr(ex, "domain", "") or "",
                        getattr(ex, "name", "") or "",
                    ]
                )
            seed_parts.append(getattr(node, "node_id", "") or "")
        seed_parts.append(str(getattr(teid, "retry_attempt", "") or ""))

    task_id = task_template.id
    if task_id is not None:
        seed_parts.extend(
            [
                getattr(task_id, "project", "") or "",
                getattr(task_id, "domain", "") or "",
                getattr(task_id, "name", "") or "",
                getattr(task_id, "version", "") or "",
            ]
        )

    seed = "-".join(p for p in seed_parts if p)
    if not seed:
        seed = "job"

    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:20]
    normalized_prefix = _INVALID_JOB_NAME_CHARS.sub("-", prefix).strip("-") or "flyte"
    suffix = f"-{digest}"
    normalized_prefix = normalized_prefix[: _MAX_JOB_NAME_LEN - len(suffix)].rstrip("-") or "j"
    return f"{normalized_prefix}{suffix}"


def _tags_to_list(tags: Optional[Dict[str, str]]) -> Optional[list]:
    if not tags:
        return None
    return [{"Key": str(k), "Value": str(v)} for k, v in tags.items()]


def _build_environment(container: Any, config: Dict[str, Any]) -> Dict[str, str]:
    """Merge the serialized task environment with explicit SageMaker overrides."""
    environment = dict(getattr(container, "env", None) or {})
    environment.update(config.get("environment") or {})
    environment[FLYTE_FAIL_ON_ERROR] = "true"
    return environment


def _container_entrypoint(container: Any) -> list[str]:
    """Validate and return the rendered entrypoint accepted by SageMaker."""
    entrypoint = list(getattr(container, "args", None) or [])
    if not entrypoint:
        raise ValueError("Pythonic SageMaker jobs require a rendered Flyte container entrypoint.")
    if len(entrypoint) > 100:
        raise ValueError("SageMaker ContainerEntrypoint supports at most 100 arguments.")
    if any(len(argument) > 256 for argument in entrypoint):
        raise ValueError("Each SageMaker ContainerEntrypoint argument must be at most 256 characters.")
    return entrypoint


@dataclass(frozen=True)
class _PythonicJobError:
    message: str
    recoverable: bool


class PythonicSageMakerJobConnector(Boto3ConnectorMixin, AsyncConnectorBase):
    """Base async connector for SageMaker Pythonic-mode jobs.

    Subclasses supply the per-service deltas (method names, response keys, state
    map) and implement ``_build_request``. ``create``/``get``/``delete`` and the
    ``outputs.pb`` / ``error.pb`` handling are shared.
    """

    # --- per-service deltas (override in subclasses) ---
    task_type_name: str = "pythonic-sagemaker-job"
    create_method: str = ""
    describe_method: str = ""
    stop_method: str = ""
    job_name_key: str = ""
    status_key: str = ""
    secondary_status_key: Optional[str] = None
    _state_map: Dict[str, Any] = field(default_factory=dict)

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name=self.task_type_name,
            metadata_type=PythonicJobMetadata,
        )

    def _build_request(
        self,
        *,
        container: Any,
        config: Dict[str, Any],
        job_name: str,
        output_prefix: str,
    ) -> Dict[str, Any]:
        """Assemble the ``create_*_job`` boto3 request. Implemented per service."""
        raise NotImplementedError

    async def create(
        self,
        task_template: TaskTemplate,
        output_prefix: str,
        inputs: Optional[LiteralMap] = None,
        task_execution_metadata: Optional[Any] = None,
        **kwargs: Any,
    ) -> PythonicJobMetadata:
        container = task_template.container
        if container is None or not getattr(container, "image", None):
            raise ValueError(
                "Pythonic mode requires a container image. Pass container_image=<ImageSpec|ECR URI> "
                "to the @task so SageMaker can run the function inside a flytekit-containing image."
            )

        config = dict(task_template.custom or {})
        region = config.get("region")
        job_name = _make_job_name(
            config.get("job_name_prefix") or "flyte-",
            task_execution_metadata,
            task_template,
            output_prefix,
        )
        request = self._build_request(
            container=container, config=config, job_name=job_name, output_prefix=output_prefix
        )

        try:
            await self._call(method=self.create_method, config=request, region=region)
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # Idempotent re-runs: SageMaker rejects duplicate job names. Treat as already-running.
            already_exists = error_code == "ResourceInUse" or (
                error_code == "ValidationException" and "Cannot create already existing" in error_message
            )
            if not already_exists:
                raise e

        interface = getattr(task_template, "interface", None)
        has_outputs = bool(getattr(interface, "outputs", None))
        return PythonicJobMetadata(
            job_name=job_name,
            output_prefix=output_prefix,
            region=region,
            has_outputs=has_outputs,
        )

    async def get(self, resource_meta: PythonicJobMetadata, **kwargs: Any) -> Resource:
        describe_response, _ = await self._call(
            method=self.describe_method,
            config={self.job_name_key: resource_meta.job_name},
            region=resource_meta.region,
        )

        current_state = describe_response.get(self.status_key)
        flyte_phase = self._state_map.get(current_state, TaskExecution.RUNNING)
        message = self._status_message(describe_response, current_state)

        if current_state in ("Completed", "Failed", "Stopped"):
            task_error = self._read_error(resource_meta.output_prefix)
            if task_error is not None:
                phase = TaskExecution.RETRYABLE_FAILED if task_error.recoverable else TaskExecution.FAILED
                return Resource(phase=phase, message=task_error.message)

        if current_state == "Completed":
            if resource_meta.has_outputs and not self._artifact_exists(resource_meta.output_prefix, "outputs.pb"):
                return Resource(
                    phase=TaskExecution.FAILED,
                    message="SageMaker job completed without producing Flyte outputs.pb.",
                )
            # outputs=None -> flytekit reads the typed return from outputs.pb.
            return Resource(phase=TaskExecution.SUCCEEDED, outputs=None, message=message)

        return Resource(phase=flyte_phase, message=message)

    async def delete(self, resource_meta: PythonicJobMetadata, **kwargs: Any) -> None:
        try:
            await self._call(
                method=self.stop_method,
                config={self.job_name_key: resource_meta.job_name},
                region=resource_meta.region,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # delete() may run after the job already finished/stopped — SageMaker
            # rejects stop on a non-running job; nothing to cancel.
            if error_code == "ResourceNotFound" or (
                error_code == "ValidationException" and "non-running" in error_message
            ):
                return
            raise e

    def _status_message(self, describe_response: Dict[str, Any], state: Optional[str]) -> Optional[str]:
        if self.secondary_status_key and state == "InProgress":
            return describe_response.get(self.secondary_status_key)
        if state in ("Failed", "Stopped"):
            secondary = describe_response.get(self.secondary_status_key) if self.secondary_status_key else None
            return describe_response.get("FailureReason") or secondary or describe_response.get("ExitMessage")
        return None

    @staticmethod
    def _artifact_exists(output_prefix: str, name: str) -> bool:
        ctx = FlyteContext.current_context()
        return ctx.file_access.exists(f"{output_prefix.rstrip('/')}/{name}")

    @classmethod
    def _read_error(cls, output_prefix: str) -> Optional[_PythonicJobError]:
        """Return structured user-error details when ``error.pb`` exists."""
        error_path = f"{output_prefix.rstrip('/')}/error.pb"
        if not cls._artifact_exists(output_prefix, "error.pb"):
            return None

        try:
            from flyteidl.core import errors_pb2

            ctx = FlyteContext.current_context()
            local_path = ctx.file_access.get_random_local_path()
            ctx.file_access.get_data(error_path, local_path)
            with open(local_path, "rb") as f:
                doc = errors_pb2.ErrorDocument()
                doc.ParseFromString(f.read())
            if doc.error and doc.error.message:
                return _PythonicJobError(
                    message=doc.error.message,
                    recoverable=doc.error.kind == errors_pb2.ContainerError.RECOVERABLE,
                )
        except Exception:
            return _PythonicJobError(
                message="User task raised an error, but error.pb could not be decoded.",
                recoverable=False,
            )
        return _PythonicJobError(
            message="User task raised an error (see error.pb in the task output prefix).",
            recoverable=False,
        )

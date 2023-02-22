from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, cast

from flyteidl.core import tasks_pb2 as _core_task
from kubernetes.client import ApiClient
from kubernetes.client.models import V1Container, V1EnvVar, V1ResourceRequirements

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.interface import Interface
from flytekit.core.pod_template import PodTemplate
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.utils import _get_container_definition
from flytekit.models import task as _task_model
from flytekit.models.security import Secret, SecurityContext

_PRIMARY_CONTAINER_NAME_FIELD = "primary_container_name"


def _sanitize_resource_name(resource: _task_model.Resources.ResourceEntry) -> str:
    return _core_task.Resources.ResourceName.Name(resource.name).lower().replace("_", "-")


class ContainerTask(PythonTask):
    """
    This is an intermediate class that represents Flyte Tasks that run a container at execution time. This is the vast
    majority of tasks - the typical ``@task`` decorated tasks for instance all run a container. An example of
    something that doesn't run a container would be something like the Athena SQL task.
    """

    class MetadataFormat(Enum):
        JSON = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_JSON
        YAML = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_YAML
        PROTO = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_PROTO

    class IOStrategy(Enum):
        DOWNLOAD_EAGER = _task_model.IOStrategy.DOWNLOAD_MODE_EAGER
        DOWNLOAD_STREAM = _task_model.IOStrategy.DOWNLOAD_MODE_STREAM
        DO_NOT_DOWNLOAD = _task_model.IOStrategy.DOWNLOAD_MODE_NO_DOWNLOAD
        UPLOAD_EAGER = _task_model.IOStrategy.UPLOAD_MODE_EAGER
        UPLOAD_ON_EXIT = _task_model.IOStrategy.UPLOAD_MODE_ON_EXIT
        DO_NOT_UPLOAD = _task_model.IOStrategy.UPLOAD_MODE_NO_UPLOAD

    def __init__(
        self,
        name: str,
        image: str,
        command: List[str],
        inputs: Optional[Dict[str, Tuple[Type, Any]]] = None,
        metadata: Optional[TaskMetadata] = None,
        arguments: Optional[List[str]] = None,
        outputs: Optional[Dict[str, Type]] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        input_data_dir: Optional[str] = None,
        output_data_dir: Optional[str] = None,
        metadata_format: MetadataFormat = MetadataFormat.JSON,
        io_strategy: Optional[IOStrategy] = None,
        secret_requests: Optional[List[Secret]] = None,
        pod_template: Optional[PodTemplate] = None,
        pod_template_name: Optional[str] = None,
        **kwargs,
    ):
        sec_ctx = None
        if secret_requests:
            for s in secret_requests:
                if not isinstance(s, Secret):
                    raise AssertionError(f"Secret {s} should be of type flytekit.Secret, received {type(s)}")
            sec_ctx = SecurityContext(secrets=secret_requests)

        # pod_template_name overwrites the metadata.pod_template_name
        kwargs["metadata"] = kwargs["metadata"] if "metadata" in kwargs else TaskMetadata()
        kwargs["metadata"].pod_template_name = pod_template_name

        super().__init__(
            task_type="raw-container",
            name=name,
            interface=Interface(inputs, outputs),
            metadata=metadata,
            task_config=None,
            security_ctx=sec_ctx,
            **kwargs,
        )
        self._image = image
        self._cmd = command
        self._args = arguments
        self._input_data_dir = input_data_dir
        self._output_data_dir = output_data_dir
        self._md_format = metadata_format
        self._io_strategy = io_strategy
        self._resources = ResourceSpec(
            requests=requests if requests else Resources(), limits=limits if limits else Resources()
        )
        self.pod_template = pod_template

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    def execute(self, **kwargs) -> Any:
        print(kwargs)
        env = ""
        for k, v in self.environment.items():
            env += f" -e {k}={v}"
        print(
            f"\ndocker run --rm -v /tmp/inputs:{self._input_data_dir} -v /tmp/outputs:{self._output_data_dir} {env}"
            f"{self._image} {self._cmd} {self._args}"
        )
        return None

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        # if pod_template is specified, return None here but in get_k8s_pod, return pod_template merged with container
        if self.pod_template is not None:
            return None

        return self._get_container(settings)

    def _get_data_loading_config(self) -> _task_model.DataLoadingConfig:
        return _task_model.DataLoadingConfig(
            input_path=self._input_data_dir,
            output_path=self._output_data_dir,
            format=self._md_format.value,
            enabled=True,
            io_strategy=self._io_strategy.value if self._io_strategy else None,
        )

    def _get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = settings.env or {}
        env = {**env, **self.environment} if self.environment else env
        return _get_container_definition(
            image=self._image,
            command=self._cmd,
            args=self._args,
            data_loading_config=self._get_data_loading_config(),
            environment=env,
            storage_request=self.resources.requests.storage,
            ephemeral_storage_request=self.resources.requests.ephemeral_storage,
            cpu_request=self.resources.requests.cpu,
            gpu_request=self.resources.requests.gpu,
            memory_request=self.resources.requests.mem,
            storage_limit=self.resources.limits.storage,
            ephemeral_storage_limit=self.resources.limits.ephemeral_storage,
            cpu_limit=self.resources.limits.cpu,
            gpu_limit=self.resources.limits.gpu,
            memory_limit=self.resources.limits.mem,
        )

    def _serialize_pod_spec(self, settings: SerializationSettings) -> Dict[str, Any]:
        containers = cast(PodTemplate, self.pod_template).pod_spec.containers
        primary_exists = False

        for container in containers:
            if container.name == cast(PodTemplate, self.pod_template).primary_container_name:
                primary_exists = True
                break

        if not primary_exists:
            # insert a placeholder primary container if it is not defined in the pod spec.
            containers.append(V1Container(name=cast(PodTemplate, self.pod_template).primary_container_name))
        final_containers = []
        for container in containers:
            # In the case of the primary container, we overwrite specific container attributes
            # with the values given to ContainerTask.
            # The attributes include: image, command, args, resource, and env (env is unioned)
            if container.name == cast(PodTemplate, self.pod_template).primary_container_name:
                prim_container = self._get_container(settings)

                container.image = self._image
                container.command = self._cmd
                container.args = self._args

                limits, requests = {}, {}
                for resource in prim_container.resources.limits:
                    limits[_sanitize_resource_name(resource)] = resource.value
                for resource in prim_container.resources.requests:
                    requests[_sanitize_resource_name(resource)] = resource.value
                resource_requirements = V1ResourceRequirements(limits=limits, requests=requests)
                if len(limits) > 0 or len(requests) > 0:
                    # Important! Only copy over resource requirements if they are non-empty.
                    container.resources = resource_requirements
                env = settings.env or {}
                env = {**env, **self.environment} if self.environment else env
                container.env = [V1EnvVar(name=key, value=val) for key, val in env.items()] + (container.env or [])
            final_containers.append(container)
        cast(PodTemplate, self.pod_template).pod_spec.containers = final_containers

        cast(PodTemplate, self.pod_template).data_config = self._get_data_loading_config()

        return ApiClient().sanitize_for_serialization(cast(PodTemplate, self.pod_template).pod_spec)

    def get_k8s_pod(self, settings: SerializationSettings) -> _task_model.K8sPod:
        if self.pod_template is None:
            return None
        return _task_model.K8sPod(
            pod_spec=self._serialize_pod_spec(settings),
            metadata=_task_model.K8sObjectMetadata(
                labels=self.pod_template.labels,
                annotations=self.pod_template.annotations,
            ),
        )

    def get_config(self, settings: SerializationSettings) -> Optional[Dict[str, str]]:
        if self.pod_template is None:
            return {}
        return {_PRIMARY_CONTAINER_NAME_FIELD: self.pod_template.primary_container_name}

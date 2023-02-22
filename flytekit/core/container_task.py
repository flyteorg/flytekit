from enum import Enum
from typing import Any, Dict, List, Optional, Type

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.interface import Interface
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.utils import _get_container_definition
from flytekit.models import task as _task_model
from flytekit.models.security import Secret, SecurityContext


# TODO: do we need pod_template here? Seems that it is a raw container not running in pods
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
        inputs: Optional[Dict[str, Type]] = None,
        metadata: Optional[TaskMetadata] = None,
        arguments: List[str] = None,
        outputs: Dict[str, Type] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        input_data_dir: str = None,
        output_data_dir: str = None,
        metadata_format: MetadataFormat = MetadataFormat.JSON,
        io_strategy: IOStrategy = None,
        secret_requests: Optional[List[Secret]] = None,
        **kwargs,
    ):
        sec_ctx = None
        if secret_requests:
            for s in secret_requests:
                if not isinstance(s, Secret):
                    raise AssertionError(f"Secret {s} should be of type flytekit.Secret, received {type(s)}")
            sec_ctx = SecurityContext(secrets=secret_requests)
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
        env = settings.env or {}
        env = {**env, **self.environment} if self.environment else env
        return _get_container_definition(
            image=self._image,
            command=self._cmd,
            args=self._args,
            data_loading_config=_task_model.DataLoadingConfig(
                input_path=self._input_data_dir,
                output_path=self._output_data_dir,
                format=self._md_format.value,
                enabled=True,
                io_strategy=self._io_strategy.value if self._io_strategy else None,
            ),
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

import os
import typing
from enum import Enum
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Type

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.context_manager import FlyteContext
from flytekit.core.interface import Interface
from flytekit.core.pod_template import PodTemplate
from flytekit.core.python_auto_container import get_registerable_container_image
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.utils import _get_container_definition, _serialize_pod_spec
from flytekit.image_spec.image_spec import ImageSpec
from flytekit.loggers import logger
from flytekit.models import task as _task_model
from flytekit.models.literals import LiteralMap
from flytekit.models.security import Secret, SecurityContext

_PRIMARY_CONTAINER_NAME_FIELD = "primary_container_name"
DOCKER_IMPORT_ERROR_MESSAGE = "Docker is not installed. Please install Docker by running `pip install docker`."


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
        image: typing.Union[str, ImageSpec],
        command: List[str],
        inputs: Optional[OrderedDict[str, Type]] = None,
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
        pod_template: Optional["PodTemplate"] = None,
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
        metadata = metadata or TaskMetadata()
        metadata.pod_template_name = pod_template_name

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
        self._outputs = outputs
        self._md_format = metadata_format
        self._io_strategy = io_strategy
        self._resources = ResourceSpec(
            requests=requests if requests else Resources(), limits=limits if limits else Resources()
        )
        self.pod_template = pod_template

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    def _extract_command_key(self, cmd: str, **kwargs) -> Any:
        """
        Extract the key from the command using regex.
        """
        import re

        input_regex = r"^\{\{\s*\.inputs\.(.*?)\s*\}\}$"
        match = re.match(input_regex, cmd)
        if match:
            return match.group(1)
        return None

    def _render_command_and_volume_binding(self, cmd: str, **kwargs) -> Tuple[str, Dict[str, Dict[str, str]]]:
        """
        We support template-style references to inputs, e.g., "{{.inputs.infile}}".
        """
        from flytekit.types.directory import FlyteDirectory
        from flytekit.types.file import FlyteFile

        command = ""
        volume_binding = {}
        k = self._extract_command_key(cmd)

        if k:
            input_val = kwargs.get(k)
            if type(input_val) in [FlyteFile, FlyteDirectory]:
                local_flyte_file_or_dir_path = str(input_val)
                remote_flyte_file_or_dir_path = os.path.join(self._input_data_dir, k.replace(".", "/"))  # type: ignore
                volume_binding[local_flyte_file_or_dir_path] = {
                    "bind": remote_flyte_file_or_dir_path,
                    "mode": "rw",
                }
                command = remote_flyte_file_or_dir_path
            else:
                command = str(input_val)
        else:
            command = cmd

        return command, volume_binding

    def _prepare_command_and_volumes(
        self, cmd_and_args: List[str], **kwargs
    ) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
        """
        Prepares the command and volume bindings for the container based on input arguments and command templates.

        Parameters:
        - cmd_and_args (List[str]): The command and arguments to prepare.
        - **kwargs: Keyword arguments representing task inputs.

        Returns:
        - Tuple[List[str], Dict[str, Dict[str, str]]]: A tuple containing the prepared commands and volume bindings.
        """

        commands = []
        volume_bindings = {}

        for cmd in cmd_and_args:
            command, volume_binding = self._render_command_and_volume_binding(cmd, **kwargs)
            commands.append(command)
            volume_bindings.update(volume_binding)

        return commands, volume_bindings

    def _pull_image_if_not_exists(self, client, image: str):
        try:
            if not client.images.list(filters={"reference": image}):
                logger.info(f"Pulling image: {image} for container task: {self.name}")
                client.images.pull(image)
        except Exception as e:
            logger.error(f"Failed to pull image {image}: {str(e)}")
            raise

    def _string_to_timedelta(self, s: str):
        import datetime
        import re

        regex = r"(?:(\d+) days?, )?(?:(\d+):)?(\d+):(\d+)(?:\.(\d+))?"
        parts = re.match(regex, s)
        if not parts:
            raise ValueError("Invalid timedelta string format")

        days = int(parts.group(1)) if parts.group(1) else 0
        hours = int(parts.group(2)) if parts.group(2) else 0
        minutes = int(parts.group(3)) if parts.group(3) else 0
        seconds = int(parts.group(4)) if parts.group(4) else 0
        microseconds = int(parts.group(5)) if parts.group(5) else 0

        return datetime.timedelta(
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            microseconds=microseconds,
        )

    def _convert_output_val_to_correct_type(self, output_val: Any, output_type: Any) -> Any:
        import datetime

        if output_type == bool:
            return output_val.lower() != "false"
        elif output_type == datetime.datetime:
            return datetime.datetime.fromisoformat(output_val)
        elif output_type == datetime.timedelta:
            return self._string_to_timedelta(output_val)
        else:
            return output_type(output_val)

    def _get_output_dict(self, output_directory: str) -> Dict[str, Any]:
        from flytekit.types.directory import FlyteDirectory
        from flytekit.types.file import FlyteFile

        output_dict = {}
        if self._outputs:
            for k, output_type in self._outputs.items():
                output_path = os.path.join(output_directory, k)
                if output_type in [FlyteFile, FlyteDirectory]:
                    output_dict[k] = output_type(path=output_path)
                else:
                    with open(output_path, "r") as f:
                        output_val = f.read()
                    output_dict[k] = self._convert_output_val_to_correct_type(output_val, output_type)
        return output_dict

    def execute(self, **kwargs) -> LiteralMap:
        try:
            import docker
        except ImportError:
            raise ImportError(DOCKER_IMPORT_ERROR_MESSAGE)

        from flytekit.core.type_engine import TypeEngine

        ctx = FlyteContext.current_context()

        # Normalize the input and output directories
        self._input_data_dir = os.path.normpath(self._input_data_dir) if self._input_data_dir else ""
        self._output_data_dir = os.path.normpath(self._output_data_dir) if self._output_data_dir else ""

        output_directory = ctx.file_access.get_random_local_directory()
        cmd_and_args = (self._cmd or []) + (self._args or [])
        commands, volume_bindings = self._prepare_command_and_volumes(cmd_and_args, **kwargs)
        volume_bindings[output_directory] = {"bind": self._output_data_dir, "mode": "rw"}

        client = docker.from_env()
        self._pull_image_if_not_exists(client, self._image)

        container = client.containers.run(
            self._image, command=commands, remove=True, volumes=volume_bindings, detach=True
        )
        # Wait for the container to finish the task
        # TODO: Add a 'timeout' parameter to control the max wait time for the container to finish the task.
        container.wait()

        output_dict = self._get_output_dict(output_directory)
        outputs_literal_map = TypeEngine.dict_to_literal_map(ctx, output_dict)
        return outputs_literal_map

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

    def _get_image(self, settings: SerializationSettings) -> str:
        if settings.fast_serialization_settings is None or not settings.fast_serialization_settings.enabled:
            if isinstance(self._image, ImageSpec):
                # Set the source root for the image spec if it's non-fast registration
                self._image.source_root = settings.source_root
        return get_registerable_container_image(self._image, settings.image_config)

    def _get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = settings.env or {}
        env = {**env, **self.environment} if self.environment else env
        return _get_container_definition(
            image=self._get_image(settings),
            command=self._cmd,
            args=self._args,
            data_loading_config=self._get_data_loading_config(),
            environment=env,
            ephemeral_storage_request=self.resources.requests.ephemeral_storage,
            cpu_request=self.resources.requests.cpu,
            gpu_request=self.resources.requests.gpu,
            memory_request=self.resources.requests.mem,
            ephemeral_storage_limit=self.resources.limits.ephemeral_storage,
            cpu_limit=self.resources.limits.cpu,
            gpu_limit=self.resources.limits.gpu,
            memory_limit=self.resources.limits.mem,
        )

    def get_k8s_pod(self, settings: SerializationSettings) -> _task_model.K8sPod:
        if self.pod_template is None:
            return None
        return _task_model.K8sPod(
            pod_spec=_serialize_pod_spec(self.pod_template, self._get_container(settings), settings),
            metadata=_task_model.K8sObjectMetadata(
                labels=self.pod_template.labels,
                annotations=self.pod_template.annotations,
            ),
            data_config=self._get_data_loading_config(),
        )

    def get_config(self, settings: SerializationSettings) -> Optional[Dict[str, str]]:
        if self.pod_template is None:
            return {}
        return {_PRIMARY_CONTAINER_NAME_FIELD: self.pod_template.primary_container_name}

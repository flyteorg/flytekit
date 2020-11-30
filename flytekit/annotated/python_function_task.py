import inspect
import re
from typing import Any, Callable, Generic, List, Optional, TypeVar

from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.context_manager import ImageConfig, RegistrationSettings
from flytekit.annotated.interface import Interface, transform_signature_to_interface
from flytekit.annotated.resources import ResourceSpec, get_resources
from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.models import task as _task_model

_IMAGE_REPLACE_REGEX = re.compile(r"({{\s*.image.(\w+).(\w+)\s*}})", re.IGNORECASE)


def get_registerable_container_image(img: Optional[str], cfg: ImageConfig) -> str:
    """
    :param img: Configured image
    :param cfg: Registration configuration
    :return:
    """
    if img is not None and img != "":
        matches = _IMAGE_REPLACE_REGEX.findall(img)
        if matches is None or len(matches) == 0:
            return img
        for m in matches:
            if len(m) < 2:
                raise AssertionError(
                    "Image specification should be of the form <fqn>:<tag> OR <fqn>:{{.image.default.version}} OR "
                    f"{{.image.xyz.fqn}}:{{.image.xyz.version}} - Received {m}"
                )
            replace_group, name, attr = m
            if name is None or name == "" or attr is None or attr == "":
                raise AssertionError(f"Image format is incorrect {m}")
            img_cfg = cfg.find_image(name)
            if img_cfg is None:
                raise AssertionError(f"Image Config with name {name} not found in the configuration")
            if attr == "version":
                if img_cfg.tag is not None:
                    img = img.replace(replace_group, img_cfg.tag)
                else:
                    img = img.replace(replace_group, cfg.default_image.tag)
            elif attr == "fqn":
                img = img.replace(replace_group, img_cfg.fqn)
            else:
                raise AssertionError(f"Only fqn and version are supported replacements, {attr} is not supported")
        return img
    return f"{cfg.default_image.fqn}:{cfg.default_image.tag}"


T = TypeVar("T")


class PythonFunctionTask(PythonTask, Generic[T]):
    """
    A Python Function task should be used as the base for all extensions that have a python function.
    This base has an interesting property, where it will auto configure the image and image version to be
    used for all the derivatives.
    """

    def __init__(
        self,
        task_config: T,
        task_function: Callable,
        metadata: _task_model.TaskMetadata,
        ignore_input_vars: List[str] = None,
        task_type="python-task",
        container_image: str = None,
        *args,
        **kwargs,
    ):
        """
        :param task_config: Configuration object for Task. Should be a unique type for that specific Task
        :param task_function: Python function that has type annotations and works for the task
        :param metadata: Task execution metadata e.g. retries, timeout etc
        :param ignore_input_vars:
        :param task_type: String task type to be associated with this Task
        :param container_image: String FQN for the image.
        """
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        mutated_interface = self._native_interface.remove_inputs(ignore_input_vars)
        super().__init__(
            task_type=task_type,
            name=f"{task_function.__module__}.{task_function.__name__}",
            interface=mutated_interface,
            metadata=metadata,
            *args,
            **kwargs,
        )
        self._task_function = task_function
        self._task_config = task_config
        self._container_image = container_image
        # TODO(katrogan): Implement resource overrides
        self._resources = get_resources(**kwargs)

    def execute(self, **kwargs) -> Any:
        return self._task_function(**kwargs)

    @property
    def native_interface(self) -> Interface:
        return self._native_interface

    @property
    def task_config(self) -> T:
        return self._task_config

    @property
    def container_image(self) -> Optional[str]:
        return self._container_image

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    def get_container(self, settings: RegistrationSettings) -> _task_model.Container:
        args = [
            "pyflyte-execute",
            "--task-module",
            self._task_function.__module__,
            "--task-name",
            self._task_function.__name__,
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
        ]
        env = settings.env
        return _get_container_definition(
            image=get_registerable_container_image(self.container_image, settings.image_config),
            command=[],
            args=args,
            data_loading_config=None,
            environment=env,
            storage_request=self._resources.requests.storage,
            cpu_request=self._resources.requests.cpu,
            gpu_request=self._resources.requests.gpu,
            memory_request=self._resources.requests.mem,
            storage_limit=self._resources.limits.storage,
            cpu_limit=self._resources.limits.cpu,
            gpu_limit=self._resources.limits.gpu,
            memory_limit=self._resources.limits.mem,
        )

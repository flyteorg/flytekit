from __future__ import annotations

import importlib
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, TypeVar

from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import ImageConfig, SerializationSettings
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.tracker import TrackedInstance
from flytekit.models import task as _task_model

T = TypeVar("T")


class FlyteTrackedABC(type(TrackedInstance), type(ABC)):
    ...


class PythonAutoContainerTask(PythonTask[T], metaclass=FlyteTrackedABC):
    """
    A Python AutoContainer task should be used as the base for all extensions that want the user's code to be in the
    container and the container information to be automatically captured.
    This base will auto configure the image and image version to be used for all its derivatives.

    If you are looking to extend, you might prefer to use ``PythonFunctionTask`` or ``PythonInstanceTask``
    """

    def __init__(
        self,
        name: str,
        task_config: T,
        task_type="python-task",
        container_image: Optional[str] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        environment: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        :param task_config: Configuration object for Task. Should be a unique type for that specific Task
        :param task_function: Python function that has type annotations and works for the task
        :param metadata: Task execution metadata e.g. retries, timeout etc
        :param ignore_input_vars:
        :param task_type: String task type to be associated with this Task
        :param container_image: String FQN for the image.
        :param Resources requests: custom resource request settings.
        :param Resources limits: custom resource limit settings.
        """
        super().__init__(
            task_type=task_type, name=name, task_config=task_config, **kwargs,
        )
        self._container_image = container_image
        # TODO(katrogan): Implement resource overrides
        self._resources = ResourceSpec(
            requests=requests if requests else Resources(), limits=limits if limits else Resources()
        )
        self._environment = environment

    @property
    def container_image(self) -> Optional[str]:
        return self._container_image

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    @abstractmethod
    def get_command(self, settings: SerializationSettings) -> List[str]:
        pass

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=get_registerable_container_image(self.container_image, settings.image_config),
            command=[],
            args=self.get_command(settings=settings),
            data_loading_config=None,
            environment=env,
            storage_request=self.resources.requests.storage,
            cpu_request=self.resources.requests.cpu,
            gpu_request=self.resources.requests.gpu,
            memory_request=self.resources.requests.mem,
            storage_limit=self.resources.limits.storage,
            cpu_limit=self.resources.limits.cpu,
            gpu_limit=self.resources.limits.gpu,
            memory_limit=self.resources.limits.mem,
        )


class TaskResolverMixin(object):
    """
    A TaskResolver that can be used to load the task itself from the actual argument that is captured.
    The argument itself should be discoverable through the class loading framework.

    .. note::

        Task Resolver can only be used for cases in which the Task can be fully loaded using constant module level variables
        and/or can be returned using the loader-args. Loader args are simple strings

    """

    @property
    @abstractmethod
    def location(self) -> str:
        pass

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        """
        Given the set of identifier keys, should return one Python Task or raise an error if not found
        """
        pass

    @abstractmethod
    def loader_args(self, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        """
        Return a list of strings that can help identify the parameter Task
        """
        pass

    @abstractmethod
    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        """
        Future proof method. Just making it easy to access all tasks (Not required today as we auto register them)
        """
        pass


class DefaultTaskResolver(TrackedInstance, TaskResolverMixin):
    def name(self) -> str:
        return "DefaultTaskResolver"

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        print(f"Default resolver, loader args {loader_args}")
        task_module = loader_args[1]
        task_name = loader_args[3]

        task_module = importlib.import_module(task_module)
        task_def = getattr(task_module, task_name)
        return task_def

    def loader_args(self, settings: SerializationSettings, task: PythonAutoContainerTask) -> List[str]:
        from flytekit.core.python_function_task import PythonFunctionTask

        if isinstance(task, PythonFunctionTask):
            return [
                "task-module",
                task.task_function.__module__,
                "task-name",
                task.task_function.__name__,
            ]
        if isinstance(task, TrackedInstance):
            return [
                "task-module",
                task.instantiated_in,
                "task-name",
                task.lhs,
            ]

    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        raise Exception("should not be needed")


default_task_resolver = DefaultTaskResolver()


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
            if len(m) < 3:
                raise AssertionError(
                    "Image specification should be of the form <fqn>:<tag> OR <fqn>:{{.image.default.version}} OR "
                    f"{{.image.xyz.fqn}}:{{.image.xyz.version}} OR {{.image.xyz}} - Received {m}"
                )
            replace_group, name, attr = m
            if name is None or name == "":
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
            elif attr == "":
                img = img.replace(replace_group, img_cfg.full)
            else:
                raise AssertionError(f"Only fqn and version are supported replacements, {attr} is not supported")
        return img
    return f"{cfg.default_image.fqn}:{cfg.default_image.tag}"


# Matches {{.image.<name>.<attr>}}. A name can be either 'default' indicating the default image passed during
# serialization or it can be a custom name for an image that must be defined in the config section Images. An attribute
# can be either 'fqn', 'version' or non-existent.
# fqn will access the fully qualified name of the image (e.g. registry/imagename:version -> registry/imagename)
# version will access the version part of the image (e.g. registry/imagename:version -> version)
# With empty attribute, it'll access the full image path (e.g. registry/imagename:version -> registry/imagename:version)
_IMAGE_REPLACE_REGEX = re.compile(r"({{\s*\.image[s]?(?:\.([a-zA-Z]+))(?:\.([a-zA-Z]+))?\s*}})", re.IGNORECASE)

import inspect
from typing import Any, Callable

from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.interface import Interface, transform_signature_to_interface
from flytekit.annotated.task import TaskPlugins, metadata
from flytekit.common.tasks.task import SdkTask
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model


class Reference(object):
    def __init__(
        self, type: int, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        self._id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)

    @property
    def id(self) -> _identifier_model.Identifier:
        return self._id


class TaskReference(Reference):
    def __init__(
        self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        super().__init__(_identifier_model.ResourceType.TASK, project, domain, name, version, *args, **kwargs)


class WorkflowReference(Reference):
    def __init__(
        self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        super().__init__(_identifier_model.ResourceType.WORKFLOW, project, domain, name, version, *args, **kwargs)


class ReferenceTask(PythonTask):
    """
    Fill in later.
    """

    def __init__(
        self,
        task_config: TaskReference,
        task_function: Callable,
        ignored_metadata: _task_model.TaskMetadata,
        *args,
        task_type="reference-task",
        **kwargs,
    ):
        self._reference = task_config
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        super().__init__(
            task_type=task_type,
            name=task_config._id._name,
            interface=self._native_interface,
            metadata=metadata(),
            *args,
            **kwargs,
        )
        self._task_function = task_function

    def execute(self, **kwargs) -> Any:
        raise Exception("Remote reference tasks cannot be run locally. You must mock this out.")

    @property
    def reference(self) -> TaskReference:
        return self._reference

    @property
    def native_interface(self) -> Interface:
        return self._native_interface

    def get_task_structure(self) -> SdkTask:
        settings = FlyteContext.current_context().registration_settings
        tk = SdkTask(
            type=self.task_type,
            metadata=self.metadata,
            interface=self.interface,
            custom=self.get_custom(settings),
            container=None,
        )
        # Reset id to ensure it matches user input
        tk._id = self._reference._id
        tk._has_registered = True
        return tk

    @property
    def id(self) -> _identifier_model.Identifier:
        return self._reference._id


TaskPlugins.register_pythontask_plugin(TaskReference, ReferenceTask)

from typing import Callable, Any

from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.task import metadata
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.reference_entity import TaskReference
from flytekit.annotated.task import TaskPlugins
from flytekit.common.tasks.task import SdkTask
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model


class ReferenceTask(PythonFunctionTask):
    """
    This is a reference task, the body of the function passed in through the constructor will never be used, only the
    signature of the function will be. The signature should also match the signature of the task you're referencing,
    as stored by Flyte Admin, if not, workflows using this will break upon compilation.
    """

    def __init__(
        self,
        task_config: TaskReference,
        task_function: Callable,
        ignored_metadata: _task_model.TaskMetadata,
        *args,
        **kwargs,
    ):
        self._reference = task_config
        super().__init__(
            task_config=task_config,
            task_type="reference-task",
            task_function=task_function,
            metadata=metadata(),
            *args,
            **kwargs,
        )
        self._name = task_config.id.name

    def execute(self, **kwargs) -> Any:
        raise Exception("Remote reference tasks cannot be run locally. You must mock this out.")

    @property
    def reference(self) -> TaskReference:
        return self._reference

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
        tk._id = self.id
        tk._has_registered = True
        return tk

    @property
    def id(self) -> _identifier_model.Identifier:
        return self.reference.id


TaskPlugins.register_pythontask_plugin(TaskReference, ReferenceTask)

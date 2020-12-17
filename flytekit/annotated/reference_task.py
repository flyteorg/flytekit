import inspect
from typing import Callable, Optional

from flytekit.annotated.interface import transform_signature_to_interface
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.reference_entity import ReferenceEntity, TaskReference
from flytekit.annotated.task import TaskPlugins
from flytekit.annotated.task import metadata as get_empty_metadata
from flytekit.common.tasks.task import SdkTask
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model


class ReferenceTask(ReferenceEntity, PythonFunctionTask):
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
        interface = transform_signature_to_interface(inspect.signature(task_function))
        super().__init__(
            _identifier_model.ResourceType.TASK,
            task_config.id.project,
            task_config.id.domain,
            task_config.id.name,
            task_config.id.version,
            interface.inputs,
            interface.outputs,
        )

        self._registerable_entity: Optional[SdkTask] = None

    def get_task_structure(self) -> SdkTask:
        # settings = FlyteContext.current_context().registration_settings
        # This is a dummy sdk task, hopefully when we clean up
        tk = SdkTask(
            type="ignore", metadata=get_empty_metadata(), interface=self.typed_interface, custom={}, container=None,
        )
        # Reset id to ensure it matches user input
        tk._id = self.id
        tk._has_registered = True
        tk.assign_name(self.reference.id.name)
        return tk


TaskPlugins.register_pythontask_plugin(TaskReference, ReferenceTask)

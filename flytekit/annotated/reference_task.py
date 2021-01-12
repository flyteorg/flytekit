import inspect
from typing import Any, Callable, Dict, Optional, Type, overload

from flytekit import TaskMetadata
from flytekit.annotated.interface import transform_signature_to_interface
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.reference_entity import ReferenceEntity, TaskReference
from flytekit.common.tasks.task import SdkTask
from flytekit.models.core import identifier as _identifier_model


class ReferenceTask(ReferenceEntity, PythonFunctionTask):
    """
    This is a reference task, the body of the function passed in through the constructor will never be used, only the
    signature of the function will be. The signature should also match the signature of the task you're referencing,
    as stored by Flyte Admin, if not, workflows using this will break upon compilation.
    """

    def __init__(
        self, project: str, domain: str, name: str, version: str, inputs: Dict[str, Type], outputs: Dict[str, Type]
    ):
        super().__init__(TaskReference(project, domain, name, version), inputs, outputs)
        self._registerable_entity: Optional[SdkTask] = None

    def get_task_structure(self) -> SdkTask:
        # settings = FlyteContext.current_context().registration_settings
        # This is a dummy sdk task, hopefully when we clean up
        tk = SdkTask(
            type="ignore",
            metadata=TaskMetadata().to_taskmetadata_model(),
            interface=self.typed_interface,
            custom={},
            container=None,
        )
        # Reset id to ensure it matches user input
        tk._id = self.id
        tk._has_registered = True
        tk.assign_name(self.reference.id.name)
        return tk


@overload
def reference_task(id: _identifier_model.Identifier) -> Callable[[Callable[..., Any]], ReferenceTask]:
    ...


@overload
def reference_task(project: str, domain: str, name: str, version: str) -> Callable[[Callable[..., Any]], ReferenceTask]:
    ...


def reference_task(
    id=None, project=None, domain=None, name=None, version=None,
):
    """
    A reference task is a pointer to a task that already exists on your Flyte installation. This
    object will not initiate a network call to Admin, which is why the user is asked to provide the expected interface.
    If at registration time the interface provided causes an issue with compilation, an error will be returned.
    """

    def wrapper(fn) -> ReferenceTask:
        interface = transform_signature_to_interface(inspect.signature(fn))
        if id is not None:
            return ReferenceTask(id.project, id.domain, id.name, id.version, interface.inputs, interface.outputs)
        return ReferenceTask(project, domain, name, version, interface.inputs, interface.outputs)

    return wrapper

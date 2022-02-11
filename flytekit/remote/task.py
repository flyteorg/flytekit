from typing import Optional

from flytekit.core import hash as _hash_mixin
from flytekit.core.interface import Interface
from flytekit.core.task import ReferenceTask
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import remote_logger as logger
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model
from flytekit.remote import interface as _interfaces


class FlyteTask(_hash_mixin.HashOnReferenceMixin, _task_model.TaskTemplate):
    """A class encapsulating a remote Flyte task."""

    def __init__(self, id, type, metadata, interface, custom, container=None, task_type_version=0, config=None):
        super(FlyteTask, self).__init__(
            id,
            type,
            metadata,
            interface,
            custom,
            container=container,
            task_type_version=task_type_version,
            config=config,
        )
        self._python_interface = None
        self._reference_entity = None

    def __call__(self, *args, **kwargs):
        if self.reference_entity is None:
            logger.warning(
                f"FlyteTask {self} is not callable, most likely because flytekit could not "
                f"guess the python interface. The workflow calling this task may not behave correctly"
            )
            return
        return self.reference_entity(*args, **kwargs)

    # TODO: Refactor behind mixin
    @property
    def reference_entity(self) -> Optional[ReferenceTask]:
        if self._reference_entity is None:
            if self.guessed_python_interface is None:
                try:
                    self.guessed_python_interface = Interface(
                        TypeEngine.guess_python_types(self.interface.inputs),
                        TypeEngine.guess_python_types(self.interface.outputs),
                    )
                except Exception as e:
                    logger.warning(f"Error backing out interface {e}, Flyte interface {self.interface}")
                    return None

            self._reference_entity = ReferenceTask(
                self.id.project,
                self.id.domain,
                self.id.name,
                self.id.version,
                inputs=self.guessed_python_interface.inputs,
                outputs=self.guessed_python_interface.outputs,
            )
        return self._reference_entity

    @property
    def interface(self) -> _interfaces.TypedInterface:
        return super(FlyteTask, self).interface

    @property
    def resource_type(self) -> _identifier_model.ResourceType:
        return _identifier_model.ResourceType.TASK

    @property
    def entity_type_text(self) -> str:
        return "Task"

    @property
    def guessed_python_interface(self) -> Optional[Interface]:
        return self._python_interface

    @guessed_python_interface.setter
    def guessed_python_interface(self, value):
        if self._python_interface is not None:
            return
        self._python_interface = value

    @classmethod
    def promote_from_model(cls, base_model: _task_model.TaskTemplate) -> "FlyteTask":
        t = cls(
            id=base_model.id,
            type=base_model.type,
            metadata=base_model.metadata,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            custom=base_model.custom,
            container=base_model.container,
            task_type_version=base_model.task_type_version,
        )
        # Override the newly generated name if one exists in the base model
        if not base_model.id.is_empty:
            t._id = base_model.id

        if t.interface is not None:
            try:
                t.guessed_python_interface = Interface(
                    inputs=TypeEngine.guess_python_types(t.interface.inputs),
                    outputs=TypeEngine.guess_python_types(t.interface.outputs),
                )
            except ValueError:
                logger.warning(f"Could not infer Python types for FlyteTask {base_model.id}")

        return t

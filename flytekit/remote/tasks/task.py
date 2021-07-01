from flytekit.common.mixins import hash as _hash_mixin
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model
from flytekit.remote import identifier as _identifier
from flytekit.remote import interface as _interfaces


class FlyteTask(_hash_mixin.HashOnReferenceMixin, _task_model.TaskTemplate):
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

    @property
    def interface(self) -> _interfaces.TypedInterface:
        return super(FlyteTask, self).interface

    @property
    def resource_type(self) -> _identifier_model.ResourceType:
        return _identifier_model.ResourceType.TASK

    @property
    def entity_type_text(self) -> str:
        return "Task"

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
            t._id = _identifier.Identifier.promote_from_model(base_model.id)

        return t

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.control_plane import identifier as _identifier
from flytekit.control_plane import interface as _interfaces
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import common as _common_model
from flytekit.models import task as _task_model
from flytekit.models.admin import common as _admin_common
from flytekit.models.core import identifier as _identifier_model


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

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project: str, domain: str, name: str, version: str) -> "FlyteTask":
        """
        This function uses the engine loader to call create a hydrated task from Admin.

        :param project:
        :param domain:
        :param name:
        :param version:
        """
        task_id = _identifier.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)
        admin_task = _flyte_engine.get_client().get_task(task_id)

        flyte_task = cls.promote_from_model(admin_task.closure.compiled_task.template)
        flyte_task._id = task_id
        return flyte_task

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch_latest(cls, project: str, domain: str, name: str) -> "FlyteTask":
        """
        This function uses the engine loader to call create a latest hydrated task from Admin.

        :param project:
        :param domain:
        :param name:
        """
        named_task = _common_model.NamedEntityIdentifier(project, domain, name)
        client = _flyte_engine.get_client()
        task_list, _ = client.list_tasks_paginated(
            named_task,
            limit=1,
            sort_by=_admin_common.Sort("created_at", _admin_common.Sort.Direction.DESCENDING),
        )
        admin_task = task_list[0] if task_list else None

        if not admin_task:
            raise _user_exceptions.FlyteEntityNotExistException("Named task {} not found".format(named_task))
        flyte_task = cls.promote_from_model(admin_task.closure.compiled_task.template)
        flyte_task._id = admin_task.id
        return flyte_task

from flyteidl.admin import task_pb2 as _admin_task

from flytekit.models import common as _common
from flytekit.models.core import identifier as _identifier
from flytekit.models.core.compiler import CompiledTask as _compiledTask
from flytekit.models.core.task import TaskTemplate


class TaskSpec(_common.FlyteIdlEntity):
    def __init__(self, template):
        """
        :param flytekit.models.core.task.TaskTemplate template:
        """
        self._template = template

    @property
    def template(self):
        """
        :rtype: flytekit.models.core.task.TaskTemplate
        """
        return self._template

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.tasks_pb2.TaskSpec
        """
        return _admin_task.TaskSpec(template=self.template.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.tasks_pb2.TaskSpec pb2_object:
        :rtype: TaskSpec
        """
        return cls(TaskTemplate.from_flyte_idl(pb2_object.template))


class TaskClosure(_common.FlyteIdlEntity):
    def __init__(self, compiled_task):
        """
        :param flytekit.models.core.compiler.CompiledTask compiled_task:
        """
        self._compiled_task = compiled_task

    @property
    def compiled_task(self):
        """
        :rtype: flytekit.models.core.compiler.CompiledTask
        """
        return self._compiled_task

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.task_pb2.TaskClosure
        """
        return _admin_task.TaskClosure(compiled_task=self.compiled_task.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.task_pb2.TaskClosure pb2_object:
        :rtype: TaskClosure
        """
        return cls(compiled_task=_compiledTask.from_flyte_idl(pb2_object.compiled_task))


class Task(_common.FlyteIdlEntity):
    def __init__(self, id, closure):
        """
        :param flytekit.models.core.identifier.Identifier id: The (project, domain, name) identifier for this task.
        :param TaskClosure closure: The closure for the underlying workload.
        """
        self._id = id
        self._closure = closure

    @property
    def id(self):
        """
        The (project, domain, name, version) identifier for this task.
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._id

    @property
    def closure(self):
        """
        The closure for the underlying workload.
        :rtype: TaskClosure
        """
        return self._closure

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.task_pb2.Task
        """
        return _admin_task.Task(
            closure=self.closure.to_flyte_idl(),
            id=self.id.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.task_pb2.Task pb2_object:
        :rtype: TaskDefinition
        """
        return cls(
            closure=TaskClosure.from_flyte_idl(pb2_object.closure),
            id=_identifier.Identifier.from_flyte_idl(pb2_object.id),
        )

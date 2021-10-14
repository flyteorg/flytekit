from flyteidl.admin import task_pb2 as _admin_task
from flyteidl.core import tasks_pb2 as _core_task

from flytekit.models import common as _common
from flytekit.models import literals as _literals
from flytekit.models.task import RuntimeMetadata as _runtimeMatadata
from flytekit.models.task import TaskTemplate as _taskTemplate
from flytekit.models.core.compiler import CompiledTask as _compiledTask

class TaskMetadata(_common.FlyteIdlEntity):
    def __init__(
        self,
        discoverable,
        runtime,
        timeout,
        retries,
        interruptible,
        discovery_version,
        deprecated_error_message,
    ):
        """
        Information needed at runtime to determine behavior such as whether or not outputs are discoverable, timeouts,
        and retries.

        :param bool discoverable: Whether or not the outputs of this task should be cached for discovery.
        :param RuntimeMetadata runtime: Metadata describing the runtime environment for this task.
        :param datetime.timedelta timeout: The amount of time to wait before timing out.  This includes queuing and
            scheduler latency.
        :param bool interruptible: Whether or not the task is interruptible.
        :param flytekit.models.literals.RetryStrategy retries: Retry strategy for this task.  0 retries means only
            try once.
        :param Text discovery_version: This is the version used to create a logical version for data in the cache.
            This is only used when `discoverable` is true.  Data is considered discoverable if: the inputs to a given
            task are the same and the discovery_version is also the same.
        :param Text deprecated: This string can be used to mark the task as deprecated.  Consumers of the task will
            receive deprecation warnings.
        """
        self._discoverable = discoverable
        self._runtime = runtime
        self._timeout = timeout
        self._interruptible = interruptible
        self._retries = retries
        self._discovery_version = discovery_version
        self._deprecated_error_message = deprecated_error_message

    @property
    def discoverable(self):
        """
        Whether or not the outputs of this task should be cached for discovery.
        :rtype: bool
        """
        return self._discoverable

    @property
    def runtime(self):
        """
        Metadata describing the runtime environment for this task.
        :rtype: RuntimeMetadata
        """
        return self._runtime

    @property
    def retries(self):
        """
        Retry strategy for this task.  0 retries means only try once.
        :rtype: flytekit.models.literals.RetryStrategy
        """
        return self._retries

    @property
    def timeout(self):
        """
        The amount of time to wait before timing out.  This includes queuing and scheduler latency.
        :rtype: datetime.timedelta
        """
        return self._timeout

    @property
    def interruptible(self):
        """
        Whether or not the task is interruptible.
        :rtype: bool
        """
        return self._interruptible

    @property
    def discovery_version(self):
        """
        This is the version used to create a logical version for data in the cache.
        This is only used when `discoverable` is true.  Data is considered discoverable if: the inputs to a given
        task are the same and the discovery_version is also the same.
        :rtype: Text
        """
        return self._discovery_version

    @property
    def deprecated_error_message(self):
        """
        This string can be used to mark the task as deprecated.  Consumers of the task will receive deprecation
        warnings.
        :rtype: Text
        """
        return self._deprecated_error_message

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.task_pb2.TaskMetadata
        """
        tm = _core_task.TaskMetadata(
            discoverable=self.discoverable,
            runtime=self.runtime.to_flyte_idl(),
            retries=self.retries.to_flyte_idl(),
            interruptible=self.interruptible,
            discovery_version=self.discovery_version,
            deprecated_error_message=self.deprecated_error_message,
        )
        if self.timeout:
            tm.timeout.FromTimedelta(self.timeout)
        return tm

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.task_pb2.TaskMetadata pb2_object:
        :rtype: TaskMetadata
        """
        return cls(
            discoverable=pb2_object.discoverable,
            runtime=_runtimeMatadata.from_flyte_idl(pb2_object.runtime),
            timeout=pb2_object.timeout.ToTimedelta(),
            interruptible=pb2_object.interruptible if pb2_object.HasField("interruptible") else None,
            retries=_literals.RetryStrategy.from_flyte_idl(pb2_object.retries),
            discovery_version=pb2_object.discovery_version,
            deprecated_error_message=pb2_object.deprecated_error_message,
        )


class TaskSpec(_common.FlyteIdlEntity):
    def __init__(self, template):
        """
        :param TaskTemplate template:
        """
        self._template = template

    @property
    def template(self):
        """
        :rtype: TaskTemplate
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
        return cls(_taskTemplate.from_flyte_idl(pb2_object.template))


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
import json as _json

from flyteidl.admin import task_pb2 as _admin_task
from flyteidl.core import tasks_pb2 as _core_task
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit.models import common as _common
from flytekit.models import literals as _literals
from flytekit.models import interface as _interface
from flytekit.models import security as _sec
from flytekit.models.task import Container as _container
from flytekit.models.task import Sql as _sql
from flytekit.models.task import K8sPod as _k8s_pod
from flytekit.models.core.compiler import CompiledTask as _compiledTask
from flytekit.models.core import identifier as _identifier

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
            runtime=RuntimeMetadata.from_flyte_idl(pb2_object.runtime),
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


class RuntimeMetadata(_common.FlyteIdlEntity):
    class RuntimeType(object):
        OTHER = 0
        FLYTE_SDK = 1

    def __init__(self, type, version, flavor):
        """
        :param int type: Enum type from RuntimeMetadata.RuntimeType
        :param Text version: Version string for SDK version.  Can be used for metrics or managing breaking changes in
            Admin or Propeller
        :param Text flavor: Optional extra information about runtime environment (e.g. Python, GoLang, etc.)
        """
        self._type = type
        self._version = version
        self._flavor = flavor

    @property
    def type(self):
        """
        Enum type from RuntimeMetadata.RuntimeType
        :rtype: int
        """
        return self._type

    @property
    def version(self):
        """
        Version string for SDK version.  Can be used for metrics or managing breaking changes in Admin or Propeller
        :rtype: Text
        """
        return self._version

    @property
    def flavor(self):
        """
        Optional extra information about runtime environment (e.g. Python, GoLang, etc.)
        :rtype: Text
        """
        return self._flavor

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.tasks_pb2.RuntimeMetadata
        """
        return _core_task.RuntimeMetadata(type=self.type, version=self.version, flavor=self.flavor)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.tasks_pb2.RuntimeMetadata pb2_object:
        :rtype: RuntimeMetadata
        """
        return cls(type=pb2_object.type, version=pb2_object.version, flavor=pb2_object.flavor)


class TaskTemplate(_common.FlyteIdlEntity):
    def __init__(
        self,
        id,
        type,
        metadata,
        interface,
        custom,
        container=None,
        task_type_version=0,
        security_context=None,
        config=None,
        k8s_pod=None,
        sql=None,
    ):
        """
        A task template represents the full set of information necessary to perform a unit of work in the Flyte system.
        It contains the metadata about what inputs and outputs are consumed or produced.  It also contains the metadata
        necessary for Flyte Propeller to do the appropriate work.

        :param flytekit.models.core.identifier.Identifier id: This is generated by the system and uniquely identifies
            the task.
        :param Text type: This is used to define additional extensions for use by Propeller or SDK.
        :param flytekit.models.admin.task.TaskMetadata metadata: This contains information needed at runtime to determine behavior such as
            whether or not outputs are discoverable, timeouts, and retries.
        :param flytekit.models.interface.TypedInterface interface: The interface definition for this task.
        :param dict[Text, T] custom: Dictionary that must be serializable to a protobuf Struct for custom task plugins.
        :param Container container: Provides the necessary entrypoint information for execution.  For instance,
            a Container might be specified with the necessary command line arguments.
        :param int task_type_version: Specific version of this task type used by plugins to potentially modify
            execution behavior or serialization.
        :param dict[str, str] config: For plugin tasks this represents additional configuration information to be used
            in tandem with the custom.
        :param dict[str, str] config: For plugin tasks this represents additional configuration information to be used
            in tandem with the custom.
        :param K8sPod k8s_pod: Alternative to the container used to execute this task.
        :param Sql sql: This is used to execute query in FlytePropeller instead of running container or k8s_pod.
        """
        if (
            (container is not None and k8s_pod is not None)
            or (container is not None and sql is not None)
            or (k8s_pod is not None and sql is not None)
        ):
            raise ValueError("At most one of container, k8s_pod or sql can be set")
        self._id = id
        self._type = type
        self._metadata = metadata
        self._interface = interface
        self._custom = custom
        self._container = container
        self._task_type_version = task_type_version
        self._config = config
        self._security_context = security_context
        self._k8s_pod = k8s_pod
        self._sql = sql

    @property
    def id(self):
        """
        This is generated by the system and uniquely identifies the task.
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._id

    @property
    def type(self):
        """
        This is used to identify additional extensions for use by Propeller or SDK.
        :rtype: Text
        """
        return self._type

    @property
    def metadata(self):
        """
        This contains information needed at runtime to determine behavior such as whether or not outputs are
        discoverable, timeouts, and retries.
        :rtype: flytekit.models.admin.task.TaskMetadata
        """
        return self._metadata

    @property
    def interface(self):
        """
        The interface definition for this task.
        :rtype: flytekit.models.interface.TypedInterface
        """
        return self._interface

    @property
    def custom(self):
        """
        Arbitrary dictionary containing metadata for custom plugins.
        :rtype: dict[Text, T]
        """
        return self._custom

    @property
    def task_type_version(self):
        return self._task_type_version

    @property
    def container(self):
        """
        If not None, the target of execution should be a container.
        :rtype: Container
        """
        return self._container

    @property
    def config(self):
        """
        Arbitrary dictionary containing metadata for parsing and handling custom plugins.
        :rtype: dict[Text, T]
        """
        return self._config

    @property
    def security_context(self):
        return self._security_context

    @property
    def k8s_pod(self):
        return self._k8s_pod

    @property
    def sql(self):
        return self._sql

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.tasks_pb2.TaskTemplate
        """
        task_template = _core_task.TaskTemplate(
            id=self.id.to_flyte_idl(),
            type=self.type,
            metadata=self.metadata.to_flyte_idl(),
            interface=self.interface.to_flyte_idl(),
            custom=_json_format.Parse(_json.dumps(self.custom), _struct.Struct()) if self.custom else None,
            container=self.container.to_flyte_idl() if self.container else None,
            task_type_version=self.task_type_version,
            security_context=self.security_context.to_flyte_idl() if self.security_context else None,
            config={k: v for k, v in self.config.items()} if self.config is not None else None,
            k8s_pod=self.k8s_pod.to_flyte_idl() if self.k8s_pod else None,
            sql=self.sql.to_flyte_idl() if self.sql else None,
        )
        return task_template

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.tasks_pb2.TaskTemplate pb2_object:
        :rtype: TaskTemplate
        """
        return cls(
            id=_identifier.Identifier.from_flyte_idl(pb2_object.id),
            type=pb2_object.type,
            metadata=TaskMetadata.from_flyte_idl(pb2_object.metadata),
            interface=_interface.TypedInterface.from_flyte_idl(pb2_object.interface),
            custom=_json_format.MessageToDict(pb2_object.custom) if pb2_object else None,
            container=_container.from_flyte_idl(pb2_object.container) if pb2_object.HasField("container") else None,
            task_type_version=pb2_object.task_type_version,
            security_context=_sec.SecurityContext.from_flyte_idl(pb2_object.security_context)
            if pb2_object.security_context
            else None,
            config={k: v for k, v in pb2_object.config.items()} if pb2_object.config is not None else None,
            k8s_pod=_k8s_pod.from_flyte_idl(pb2_object.k8s_pod) if pb2_object.HasField("k8s_pod") else None,
            sql=_sql.from_flyte_idl(pb2_object.sql) if pb2_object.HasField("sql") else None,
        )

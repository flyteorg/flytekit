import json as _json
import typing

import six as _six
from flyteidl.core import tasks_pb2 as _core_task, literals_pb2 as _literals_pb2
from google.protobuf import json_format as _json_format, struct_pb2 as _struct

from flytekit.models import common as _common
from flytekit.models.admin.core.task import RuntimeMetadata
from flytekit.models.core import literals as _literals, identifier as _identifier, interface as _interface, \
    security as _sec
from flytekit.plugins import flyteidl as _lazy_flyteidl


class Resources(_common.FlyteIdlEntity):
    class ResourceName(object):
        UNKNOWN = _core_task.Resources.UNKNOWN
        CPU = _core_task.Resources.CPU
        GPU = _core_task.Resources.GPU
        MEMORY = _core_task.Resources.MEMORY
        STORAGE = _core_task.Resources.STORAGE
        EPHEMERAL_STORAGE = _core_task.Resources.EPHEMERAL_STORAGE

    class ResourceEntry(_common.FlyteIdlEntity):
        def __init__(self, name, value):
            """
            :param int name: enum value from ResourceName
            :param Text value: a textual value describing the resource need.  Must be a valid k8s quantity.
            """
            self._name = name
            self._value = value

        @property
        def name(self):
            """
            enum value from ResourceName
            :rtype: int
            """
            return self._name

        @property
        def value(self):
            """
            A textual value describing the resource need.  Must be a valid k8s quantity.
            :rtype: Text
            """
            return self._value

        def to_flyte_idl(self):
            """
            :rtype: flyteidl.core.tasks_pb2.ResourceEntry
            """
            return _core_task.Resources.ResourceEntry(name=self.name, value=self.value)

        @classmethod
        def from_flyte_idl(cls, pb2_object):
            """
            :param flyteidl.core.tasks_pb2.Resources.ResourceEntry pb2_object:
            :rtype: Resources.ResourceEntry
            """
            return cls(name=pb2_object.name, value=pb2_object.value)

    def __init__(self, requests, limits):
        """
        :param list[Resources.ResourceEntry] requests: The desired resources for execution.  This is given on a best
            effort basis.
        :param list[Resources.ResourceEntry] limits: These are the limits required.  These are guaranteed to be
            satisfied.
        """
        self._requests = requests
        self._limits = limits

    @property
    def requests(self):
        """
        The desired resources for execution.  This is given on a best effort basis.
        :rtype: list[Resources.ResourceEntry]
        """
        return self._requests

    @property
    def limits(self):
        """
        These are the limits required.  These are guaranteed to be satisfied.
        :rtype: list[Resources.ResourceEntry]
        """
        return self._limits

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.tasks_pb2.Resources
        """
        return _core_task.Resources(
            requests=[r.to_flyte_idl() for r in self.requests],
            limits=[r.to_flyte_idl() for r in self.limits],
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.tasks_pb2.Resources.ResourceEntry pb2_object:
        :rtype: Resources
        """
        return cls(
            requests=[Resources.ResourceEntry.from_flyte_idl(r) for r in pb2_object.requests],
            limits=[Resources.ResourceEntry.from_flyte_idl(l) for l in pb2_object.limits],
        )


class IOStrategy(_common.FlyteIdlEntity):
    """
    Provides methods to manage data in and out of the Raw container using Download Modes. This can only be used if DataLoadingConfig is enabled.
    """

    DOWNLOAD_MODE_EAGER = _core_task.IOStrategy.DOWNLOAD_EAGER
    DOWNLOAD_MODE_STREAM = _core_task.IOStrategy.DOWNLOAD_STREAM
    DOWNLOAD_MODE_NO_DOWNLOAD = _core_task.IOStrategy.DO_NOT_DOWNLOAD

    UPLOAD_MODE_EAGER = _core_task.IOStrategy.UPLOAD_EAGER
    UPLOAD_MODE_ON_EXIT = _core_task.IOStrategy.UPLOAD_ON_EXIT
    UPLOAD_MODE_NO_UPLOAD = _core_task.IOStrategy.DO_NOT_UPLOAD

    def __init__(
        self,
        download_mode: _core_task.IOStrategy.DownloadMode = DOWNLOAD_MODE_EAGER,
        upload_mode: _core_task.IOStrategy.UploadMode = UPLOAD_MODE_ON_EXIT,
    ):
        self._download_mode = download_mode
        self._upload_mode = upload_mode

    def to_flyte_idl(self) -> _core_task.IOStrategy:
        return _core_task.IOStrategy(download_mode=self._download_mode, upload_mode=self._upload_mode)

    @classmethod
    def from_flyte_idl(cls, pb2_object: _core_task.IOStrategy):
        if pb2_object is None:
            return None
        return cls(
            download_mode=pb2_object.download_mode,
            upload_mode=pb2_object.upload_mode,
        )


class DataLoadingConfig(_common.FlyteIdlEntity):
    LITERALMAP_FORMAT_PROTO = _core_task.DataLoadingConfig.PROTO
    LITERALMAP_FORMAT_JSON = _core_task.DataLoadingConfig.JSON
    LITERALMAP_FORMAT_YAML = _core_task.DataLoadingConfig.YAML
    _LITERALMAP_FORMATS = frozenset([LITERALMAP_FORMAT_JSON, LITERALMAP_FORMAT_PROTO, LITERALMAP_FORMAT_YAML])

    def __init__(
        self,
        input_path: str,
        output_path: str,
        enabled: bool = True,
        format: _core_task.DataLoadingConfig.LiteralMapFormat = LITERALMAP_FORMAT_PROTO,
        io_strategy: IOStrategy = None,
    ):
        if format not in self._LITERALMAP_FORMATS:
            raise ValueError(
                "Metadata format {} not supported. Should be one of {}".format(format, self._LITERALMAP_FORMATS)
            )
        self._input_path = input_path
        self._output_path = output_path
        self._enabled = enabled
        self._format = format
        self._io_strategy = io_strategy

    def to_flyte_idl(self) -> _core_task.DataLoadingConfig:
        return _core_task.DataLoadingConfig(
            input_path=self._input_path,
            output_path=self._output_path,
            format=self._format,
            enabled=self._enabled,
            io_strategy=self._io_strategy.to_flyte_idl() if self._io_strategy is not None else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2: _core_task.DataLoadingConfig) -> "DataLoadingConfig":
        if pb2 is None:
            return None
        return cls(
            input_path=pb2.input_path,
            output_path=pb2.output_path,
            enabled=pb2.enabled,
            format=pb2.format,
            io_strategy=IOStrategy.from_flyte_idl(pb2.io_strategy) if pb2.HasField("io_strategy") else None,
        )


class Container(_common.FlyteIdlEntity):
    def __init__(self, image, command, args, resources, env, config, data_loading_config=None):
        """
        This defines a container target.  It will execute the appropriate command line on the appropriate image with
        the given configurations.

        :param Text image: The fully-qualified identifier for the image.
        :param list[Text] command: A list of 'words' for the command.  i.e. ['aws', 's3', 'ls']
        :param list[Text] args: A list of arguments for the command.  i.e. ['s3://some/path', '/tmp/local/path']
        :param flytekit.models.core.task.Resources resources: A definition of requisite compute resources.
        :param dict[Text, Text] env: A definition of key-value pairs for environment variables.
        :param dict[Text, Text] config: A definition of configuration key-value pairs.
        :type DataLoadingConfig data_loading_config: object
        """
        self._data_loading_config = data_loading_config
        self._image = image
        self._command = command
        self._args = args
        self._resources = resources
        self._env = env
        self._config = config

    @property
    def image(self):
        """
        The fully-qualified identifier for the image.
        :rtype: Text
        """
        return self._image

    @property
    def command(self):
        """
        A list of 'words' for the command.  i.e. ['aws', 's3', 'ls']
        :rtype: list[Text]
        """
        return self._command

    @property
    def args(self):
        """
         A list of arguments for the command.  i.e. ['s3://some/path', '/tmp/local/path']
        :rtype: list[Text]
        """
        return self._args

    @property
    def resources(self):
        """
        A definition of requisite compute resources.
        :rtype: flytekit.models.core.task.Resources
        """
        return self._resources

    @property
    def env(self):
        """
        A definition of key-value pairs for environment variables.  Currently, only str->str is
            supported.
        :rtype: dict[Text, Text]
        """
        return self._env

    @property
    def config(self):
        """
        A definition of key-value pairs for configuration.  Currently, only str->str is
            supported.
        :rtype: dict[Text, Text]
        """
        return self._config

    @property
    def data_loading_config(self):
        """
        :rtype: flytekit.models.core.task.DataLoadingConfig
        """
        return self._data_loading_config

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.tasks_pb2.Container
        """
        return _core_task.Container(
            image=self.image,
            command=self.command,
            args=self.args,
            resources=self.resources.to_flyte_idl(),
            env=[_literals_pb2.KeyValuePair(key=k, value=v) for k, v in _six.iteritems(self.env)],
            config=[_literals_pb2.KeyValuePair(key=k, value=v) for k, v in _six.iteritems(self.config)],
            data_config=self._data_loading_config.to_flyte_idl() if self._data_loading_config else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.tasks_pb2.Container pb2_object:
        :rtype: Container
        """
        return cls(
            image=pb2_object.image,
            command=pb2_object.command,
            args=pb2_object.args,
            resources=Resources.from_flyte_idl(pb2_object.resources),
            env={kv.key: kv.value for kv in pb2_object.env},
            config={kv.key: kv.value for kv in pb2_object.config},
            data_loading_config=DataLoadingConfig.from_flyte_idl(pb2_object.data_config)
            if pb2_object.HasField("data_config")
            else None,
        )


class K8sObjectMetadata(_common.FlyteIdlEntity):
    def __init__(self, labels: typing.Dict[str, str] = None, annotations: typing.Dict[str, str] = None):
        """
        This defines additional metadata for building a kubernetes pod.
        """
        self._labels = labels
        self._annotations = annotations

    @property
    def labels(self) -> typing.Dict[str, str]:
        return self._labels

    @property
    def annotations(self) -> typing.Dict[str, str]:
        return self._annotations

    def to_flyte_idl(self) -> _core_task.K8sObjectMetadata:
        return _core_task.K8sObjectMetadata(
            labels={k: v for k, v in self.labels.items()} if self.labels is not None else None,
            annotations={k: v for k, v in self.annotations.items()} if self.annotations is not None else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _core_task.K8sObjectMetadata):
        return cls(
            labels={k: v for k, v in pb2_object.labels.items()} if pb2_object.labels is not None else None,
            annotations={k: v for k, v in pb2_object.annotations.items()}
            if pb2_object.annotations is not None
            else None,
        )


class K8sPod(_common.FlyteIdlEntity):
    def __init__(self, metadata: K8sObjectMetadata = None, pod_spec: typing.Dict[str, typing.Any] = None):
        """
        This defines a kubernetes pod target.  It will build the pod target during task execution
        """
        self._metadata = metadata
        self._pod_spec = pod_spec

    @property
    def metadata(self) -> K8sObjectMetadata:
        return self._metadata

    @property
    def pod_spec(self) -> typing.Dict[str, typing.Any]:
        return self._pod_spec

    def to_flyte_idl(self) -> _core_task.K8sPod:
        return _core_task.K8sPod(
            metadata=self._metadata.to_flyte_idl(),
            pod_spec=_json_format.Parse(_json.dumps(self.pod_spec), _struct.Struct()) if self.pod_spec else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _core_task.K8sPod):
        return cls(
            metadata=K8sObjectMetadata.from_flyte_idl(pb2_object.metadata),
            pod_spec=_json_format.MessageToDict(pb2_object.pod_spec) if pb2_object.HasField("pod_spec") else None,
        )


class Sql(_common.FlyteIdlEntity):
    class Dialect(object):
        ANSI = 0
        HIVE = 1

    def __init__(self, statement: str = None, dialect: int = 0):
        """
        This defines a kubernetes pod target. It will build the pod target during task execution
        """
        self._statement = statement
        self._dialect = dialect

    @property
    def statement(self) -> str:
        return self._statement

    @property
    def dialect(self) -> int:
        return self._dialect

    def to_flyte_idl(self) -> _core_task.Sql:
        return _core_task.Sql(statement=self.statement, dialect=self.dialect)

    @classmethod
    def from_flyte_idl(cls, pb2_object: _core_task.Sql):
        return cls(
            statement=pb2_object.statement,
            dialect=pb2_object.dialect,
        )


class SidecarJob(_common.FlyteIdlEntity):
    def __init__(self, pod_spec, primary_container_name, annotations=None, labels=None):
        """
        A sidecar job represents the full kubernetes pod spec and related metadata required for executing a sidecar
        task.

        :param pod_spec: k8s.io.api.core.v1.PodSpec
        :param primary_container_name: Text
        :param dict[Text, Text] annotations:
        :param dict[Text, Text] labels:
        """
        self._pod_spec = pod_spec
        self._primary_container_name = primary_container_name
        self._annotations = annotations
        self._labels = labels

    @property
    def pod_spec(self):
        """
        :rtype: k8s.io.api.core.v1.PodSpec
        """
        return self._pod_spec

    @property
    def primary_container_name(self):
        """
        :rtype: Text
        """
        return self._primary_container_name

    @property
    def annotations(self):
        """
        :rtype: dict[Text,Text]
        """
        return self._annotations

    @property
    def labels(self):
        """
        :rtype: dict[Text,Text]
        """
        return self._labels

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.tasks_pb2.SidecarJob
        """
        return _lazy_flyteidl.plugins.sidecar_pb2.SidecarJob(
            pod_spec=self.pod_spec,
            primary_container_name=self.primary_container_name,
            annotations=self.annotations,
            labels=self.labels,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.task_pb2.Task pb2_object:
        :rtype: flytekit.models.core.task.Container
        """
        return cls(
            pod_spec=pb2_object.pod_spec,
            primary_container_name=pb2_object.primary_container_name,
            annotations=pb2_object.annotations,
            labels=pb2_object.labels,
        )


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
        :param flytekit.models.admin.core.task.RuntimeMetadata runtime: Metadata describing the runtime environment for this task.
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
        :rtype: flytekit.models.admin.core.task.RuntimeMetadata
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
        :rtype: flyteidl.core.task_pb2.TaskMetadata
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
        :param flytekit.models.core.task.TaskMetadata metadata: This contains information needed at runtime to determine behavior such as
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
        :rtype: flytekit.models.core.task.TaskMetadata
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
            container=Container.from_flyte_idl(pb2_object.container) if pb2_object.HasField("container") else None,
            task_type_version=pb2_object.task_type_version,
            security_context=_sec.SecurityContext.from_flyte_idl(pb2_object.security_context)
            if pb2_object.security_context
            else None,
            config={k: v for k, v in pb2_object.config.items()} if pb2_object.config is not None else None,
            k8s_pod=K8sPod.from_flyte_idl(pb2_object.k8s_pod) if pb2_object.HasField("k8s_pod") else None,
            sql=Sql.from_flyte_idl(pb2_object.sql) if pb2_object.HasField("sql") else None,
        )
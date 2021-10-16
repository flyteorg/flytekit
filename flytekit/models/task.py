import json as _json
import typing

import six as _six
from flyteidl.core import literals_pb2 as _literals_pb2
from flyteidl.core import tasks_pb2 as _core_task
from flyteidl.plugins import pytorch_pb2 as _pytorch_task
from flyteidl.plugins import spark_pb2 as _spark_task
from flyteidl.plugins import tensorflow_pb2 as _tensorflow_task
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import common as _common
from flytekit.plugins import flyteidl as _lazy_flyteidl
from flytekit.sdk.spark_types import SparkType as _spark_type


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


class SparkJob(_common.FlyteIdlEntity):
    def __init__(
        self,
        spark_type,
        application_file,
        main_class,
        spark_conf,
        hadoop_conf,
        executor_path,
    ):
        """
        This defines a SparkJob target.  It will execute the appropriate SparkJob.

        :param application_file: The main application file to execute.
        :param dict[Text, Text] spark_conf: A definition of key-value pairs for spark config for the job.
        :param dict[Text, Text] hadoop_conf: A definition of key-value pairs for hadoop config for the job.
        """
        self._application_file = application_file
        self._spark_type = spark_type
        self._main_class = main_class
        self._executor_path = executor_path
        self._spark_conf = spark_conf
        self._hadoop_conf = hadoop_conf

    def with_overrides(
        self, new_spark_conf: typing.Dict[str, str] = None, new_hadoop_conf: typing.Dict[str, str] = None
    ) -> "SparkJob":
        if not new_spark_conf:
            new_spark_conf = self.spark_conf

        if not new_hadoop_conf:
            new_hadoop_conf = self.hadoop_conf

        return SparkJob(
            spark_type=self.spark_type,
            application_file=self.application_file,
            main_class=self.main_class,
            spark_conf=new_spark_conf,
            hadoop_conf=new_hadoop_conf,
            executor_path=self.executor_path,
        )

    @property
    def main_class(self):
        """
        The main class to execute
        :rtype: Text
        """
        return self._main_class

    @property
    def spark_type(self):
        """
        Spark Job Type
        :rtype: Text
        """
        return self._spark_type

    @property
    def application_file(self):
        """
        The main application file to execute
        :rtype: Text
        """
        return self._application_file

    @property
    def executor_path(self):
        """
        The python executable to use
        :rtype: Text
        """
        return self._executor_path

    @property
    def spark_conf(self):
        """
        A definition of key-value pairs for spark config for the job.
         :rtype: dict[Text, Text]
        """
        return self._spark_conf

    @property
    def hadoop_conf(self):
        """
         A definition of key-value pairs for hadoop config for the job.
        :rtype: dict[Text, Text]
        """
        return self._hadoop_conf

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.plugins.spark_pb2.SparkJob
        """

        if self.spark_type == _spark_type.PYTHON:
            application_type = _spark_task.SparkApplication.PYTHON
        elif self.spark_type == _spark_type.JAVA:
            application_type = _spark_task.SparkApplication.JAVA
        elif self.spark_type == _spark_type.SCALA:
            application_type = _spark_task.SparkApplication.SCALA
        elif self.spark_type == _spark_type.R:
            application_type = _spark_task.SparkApplication.R
        else:
            raise _user_exceptions.FlyteValidationException("Invalid Spark Application Type Specified")

        return _spark_task.SparkJob(
            applicationType=application_type,
            mainApplicationFile=self.application_file,
            mainClass=self.main_class,
            executorPath=self.executor_path,
            sparkConf=self.spark_conf,
            hadoopConf=self.hadoop_conf,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.plugins.spark_pb2.SparkJob pb2_object:
        :rtype: SparkJob
        """

        application_type = _spark_type.PYTHON
        if pb2_object.type == _spark_task.SparkApplication.JAVA:
            application_type = _spark_type.JAVA
        elif pb2_object.type == _spark_task.SparkApplication.SCALA:
            application_type = _spark_type.SCALA
        elif pb2_object.type == _spark_task.SparkApplication.R:
            application_type = _spark_type.R

        return cls(
            type=application_type,
            spark_conf=pb2_object.sparkConf,
            application_file=pb2_object.mainApplicationFile,
            main_class=pb2_object.mainClass,
            hadoop_conf=pb2_object.hadoopConf,
            executor_path=pb2_object.executorPath,
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
        :param Resources resources: A definition of requisite compute resources.
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
        :rtype: Resources
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
        :rtype: DataLoadingConfig
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
        :param flyteidl.admin.task_pb2.Task pb2_object:
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
        :rtype: Container
        """
        return cls(
            pod_spec=pb2_object.pod_spec,
            primary_container_name=pb2_object.primary_container_name,
            annotations=pb2_object.annotations,
            labels=pb2_object.labels,
        )


class PyTorchJob(_common.FlyteIdlEntity):
    def __init__(self, workers_count):
        self._workers_count = workers_count

    @property
    def workers_count(self):
        return self._workers_count

    def to_flyte_idl(self):
        return _pytorch_task.DistributedPyTorchTrainingTask(
            workers=self.workers_count,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            workers_count=pb2_object.workers,
        )


class TensorFlowJob(_common.FlyteIdlEntity):
    def __init__(self, workers_count, ps_replicas_count, chief_replicas_count):
        self._workers_count = workers_count
        self._ps_replicas_count = ps_replicas_count
        self._chief_replicas_count = chief_replicas_count

    @property
    def workers_count(self):
        return self._workers_count

    @property
    def ps_replicas_count(self):
        return self._ps_replicas_count

    @property
    def chief_replicas_count(self):
        return self._chief_replicas_count

    def to_flyte_idl(self):
        return _tensorflow_task.DistributedTensorflowTrainingTask(
            workers=self.workers_count, ps_replicas=self.ps_replicas_count, chief_replicas=self.chief_replicas_count
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            workers_count=pb2_object.workers,
            ps_replicas_count=pb2_object.ps_replicas,
            chief_replicas_count=pb2_object.chief_replicas,
        )

import typing

from flyteidl.plugins import spark_pb2 as _spark_task

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import common as _common
from flytekit.sdk.spark_types import SparkType as _spark_type


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

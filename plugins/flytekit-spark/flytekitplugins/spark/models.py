import enum
from typing import Dict, Optional

from flyteidl.plugins import spark_pb2 as _spark_task
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.core.pod_template import PodTemplate
from flytekit.exceptions import user as _user_exceptions
from flytekit.models import common as _common


class SparkType(enum.Enum):
    PYTHON = 1
    SCALA = 2
    JAVA = 3
    R = 4


class SparkJob(_common.FlyteIdlEntity):
    def __init__(
        self,
        spark_type: SparkType,
        application_file: str,
        main_class: str,
        spark_conf: Dict[str, str],
        hadoop_conf: Dict[str, str],
        executor_path: str,
        databricks_conf: Dict[str, Dict[str, Dict]] = {},
        databricks_token: Optional[str] = None,
        databricks_instance: Optional[str] = None,
        driver_pod_template: Optional["PodTemplate"] = None,
        driver_pod_template_name: Optional[str] = None,
        executor_pod_template: Optional["PodTemplate"] = None,
        executor_pod_template_name: Optional[str] = None,
    ):
        """
        This defines a SparkJob target.  It will execute the appropriate SparkJob.

        :param application_file: The main application file to execute.
        :param dict[Text, Text] spark_conf: A definition of key-value pairs for spark config for the job.
        :param dict[Text, Text] hadoop_conf: A definition of key-value pairs for hadoop config for the job.
        :param Optional[dict[Text, dict]] databricks_conf: A definition of key-value pairs for databricks config for the job. Refer to https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit.
        :param Optional[str] databricks_token: databricks access token.
        :param Optional[str] databricks_instance: Domain name of your deployment. Use the form <account>.cloud.databricks.com.
        """
        self._application_file = application_file
        self._spark_type = spark_type
        self._main_class = main_class
        self._executor_path = executor_path
        self._spark_conf = spark_conf
        self._hadoop_conf = hadoop_conf
        self._databricks_conf = databricks_conf
        self._databricks_token = databricks_token
        self._databricks_instance = databricks_instance
        self._driver_pod_template = driver_pod_template
        self._driver_pod_template_name = driver_pod_template_name
        self._exector_pod_template = executor_pod_template
        self._exector_pod_template_name = executor_pod_template_name

    def with_overrides(
        self,
        new_spark_conf: Optional[Dict[str, str]] = None,
        new_hadoop_conf: Optional[Dict[str, str]] = None,
        new_databricks_conf: Optional[Dict[str, Dict]] = None,
        new_driver_pod_template: Optional["PodTemplate"] = None,
        new_driver_pod_template_name: Optional[str] = None,
        new_executor_pod_template: Optional["PodTemplate"] = None,
        new_executor_pod_template_name: Optional[str] = None,
    ) -> "SparkJob":
        if not new_spark_conf:
            new_spark_conf = self.spark_conf

        if not new_hadoop_conf:
            new_hadoop_conf = self.hadoop_conf

        if not new_databricks_conf:
            new_databricks_conf = self.databricks_conf

        if not new_driver_pod_template:
            new_driver_pod_template = self._driver_pod_template

        if not new_driver_pod_template_name:
            new_driver_pod_template_name = self._driver_pod_template_name

        if not new_executor_pod_template:
            new_executor_pod_template = self._exector_pod_template

        if not new_executor_pod_template_name:
            new_executor_pod_template_name = self._exector_pod_template_name

        return SparkJob(
            spark_type=self.spark_type,
            application_file=self.application_file,
            main_class=self.main_class,
            spark_conf=new_spark_conf,
            hadoop_conf=new_hadoop_conf,
            databricks_conf=new_databricks_conf,
            databricks_token=self.databricks_token,
            databricks_instance=self.databricks_instance,
            executor_path=self.executor_path,
            driver_pod_template=new_driver_pod_template,
            driver_pod_template_name=new_driver_pod_template_name,
            executor_pod_template=new_executor_pod_template,
            executor_pod_template_name=new_executor_pod_template_name,
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

    @property
    def databricks_conf(self) -> Dict[str, Dict]:
        """
        databricks_conf: Databricks job configuration.
        Config structure can be found here. https://docs.databricks.com/dev-tools/api/2.0/jobs.html#request-structure
        :rtype: dict[Text, dict[Text, Text]]
        """
        return self._databricks_conf

    @property
    def databricks_token(self) -> str:
        """
        Databricks access token
        :rtype: str
        """
        return self._databricks_token

    @property
    def databricks_instance(self) -> str:
        """
        Domain name of your deployment. Use the form <account>.cloud.databricks.com.
        :rtype: str
        """
        return self._databricks_instance

    @property
    def driver_pod_template(self) -> Optional["PodTemplate"]:
        """
        The PodTemplate to use for the driver.
        :rtype: Optional[PodTemplate]
        """
        return self._driver_pod_template

    @property
    def driver_pod_template_name(self) -> Optional[str]:
        """
        The name of the existing PodTemplate resource to use for the driver.
        :rtype: Optional[str]
        """
        return self._driver_pod_template_name

    @property
    def executor_pod_template(self) -> Optional["PodTemplate"]:
        """
        The PodTemplate to use for the executor.
        :rtype: Optional[PodTemplate]
        """
        return self._exector_pod_template

    @property
    def executor_pod_template_name(self) -> Optional[str]:
        """
        The name of the existing PodTemplate resource to use for the executor.
        :rtype: Optional[str]
        """
        return self._exector_pod_template_name

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.plugins.spark_pb2.SparkJob
        """

        if self.spark_type == SparkType.PYTHON:
            application_type = _spark_task.SparkApplication.PYTHON
        elif self.spark_type == SparkType.JAVA:
            application_type = _spark_task.SparkApplication.JAVA
        elif self.spark_type == SparkType.SCALA:
            application_type = _spark_task.SparkApplication.SCALA
        elif self.spark_type == SparkType.R:
            application_type = _spark_task.SparkApplication.R
        else:
            raise _user_exceptions.FlyteValidationException("Invalid Spark Application Type Specified")

        databricks_conf = Struct()
        databricks_conf.update(self.databricks_conf)

        return _spark_task.SparkJob(
            applicationType=application_type,
            mainApplicationFile=self.application_file,
            mainClass=self.main_class,
            executorPath=self.executor_path,
            sparkConf=self.spark_conf,
            hadoopConf=self.hadoop_conf,
            databricksConf=databricks_conf,
            databricksToken=self.databricks_token,
            databricksInstance=self.databricks_instance,
            # TODO: pod templates
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.plugins.spark_pb2.SparkJob pb2_object:
        :rtype: SparkJob
        """

        application_type = SparkType.PYTHON
        if pb2_object.type == _spark_task.SparkApplication.JAVA:
            application_type = SparkType.JAVA
        elif pb2_object.type == _spark_task.SparkApplication.SCALA:
            application_type = SparkType.SCALA
        elif pb2_object.type == _spark_task.SparkApplication.R:
            application_type = SparkType.R

        return cls(
            type=application_type,
            spark_conf=pb2_object.sparkConf,
            application_file=pb2_object.mainApplicationFile,
            main_class=pb2_object.mainClass,
            hadoop_conf=pb2_object.hadoopConf,
            executor_path=pb2_object.executorPath,
            databricks_conf=json_format.MessageToDict(pb2_object.databricksConf),
            databricks_token=pb2_object.databricksToken,
            databricks_instance=pb2_object.databricksInstance,
            # TODO: pod templates
        )

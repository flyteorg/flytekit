from __future__ import absolute_import

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec

from flytekit import __version__
import sys as _sys
import six as _six
from flytekit.common.tasks import task as _base_tasks
from flytekit.models import literals as _literal_models, task as _task_models
from google.protobuf.json_format import MessageToDict as _MessageToDict
from flytekit.common import interface as _interface
from flytekit.models import interface as _interface_model
from flytekit.configuration import internal as _internal_config

class SdkGenericSparkTask( _base_tasks.SdkTask):
    """
    This class includes the additional logic for building a task that executes as a Spark Job.

    """
    def __init__(
            self,
            task_type,
            discovery_version,
            retries,
            interruptible,
            task_inputs,
            deprecated,
            discoverable,
            timeout,
            spark_type,
            main_class,
            main_application_file,
            spark_conf,
            hadoop_conf,
            environment,
    ):
        """
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param bool interruptible: Whether or not task is interruptible
        :param Text deprecated:
        :param bool discoverable:
        :param datetime.timedelta timeout:
        :param Text spark_type: Type of Spark Job: Scala/Java
        :param Text main_class: Main class to execute for Scala/Java jobs
        :param Text main_application_file: Main application file
        :param dict[Text,Text] spark_conf:
        :param dict[Text,Text] hadoop_conf:
        :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.
        """

        spark_job = _task_models.SparkJob(
            spark_conf=spark_conf,
            hadoop_conf=hadoop_conf,
            type = spark_type,
            application_file=main_application_file,
            main_class=main_class,
            executor_path=_sys.executable,
        ).to_flyte_idl()

        # No output support
        input_variables = {k: _interface_model.Variable(v.to_flyte_literal_type(), k) for k, v in _six.iteritems(task_inputs)}

        super(SdkGenericSparkTask, self).__init__(
            task_type,
            _task_models.TaskMetadata(
                discoverable,
                _task_models.RuntimeMetadata(
                    _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                    __version__,
                    'spark'
                ),
                timeout,
                _literal_models.RetryStrategy(retries),
                interruptible,
                discovery_version,
                deprecated
            ),
            _interface.TypedInterface(input_variables, {}),
            _MessageToDict(spark_job),
            container=self._get_container_definition(
                task_inputs= task_inputs,
                environment=environment
            )
        )

    def _get_container_definition(
            self,
            task_inputs=None,
            environment=None,
    ):
        """
        :rtype: Container
        """

        args = []
        for k, v in _six.iteritems(task_inputs):
            args.append("--{}".format(k))
            args.append("{{{{.Inputs.{}}}}}".format(k))

        return _task_models.Container(
            image= _internal_config.IMAGE.get(),
            command=[],
            args=args,
            resources=_task_models.Resources([], []),
            env=environment,
            config={}
        )

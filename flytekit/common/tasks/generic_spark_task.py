import sys as _sys

import six as _six
from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit import __version__
from flytekit.common import interface as _interface
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import task as _base_tasks
from flytekit.common.types import helpers as _helpers
from flytekit.common.types import primitives as _primitives
from flytekit.configuration import internal as _internal_config
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models

input_types_supported = {
    _primitives.Integer,
    _primitives.Boolean,
    _primitives.Float,
    _primitives.String,
    _primitives.Datetime,
    _primitives.Timedelta,
}


class SdkGenericSparkTask(_base_tasks.SdkTask):
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
            spark_type=spark_type,
            application_file=main_application_file,
            main_class=main_class,
            executor_path=_sys.executable,
        ).to_flyte_idl()

        super(SdkGenericSparkTask, self).__init__(
            task_type,
            _task_models.TaskMetadata(
                discoverable,
                _task_models.RuntimeMetadata(
                    _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                    __version__,
                    "spark",
                ),
                timeout,
                _literal_models.RetryStrategy(retries),
                interruptible,
                discovery_version,
                deprecated,
            ),
            _interface.TypedInterface({}, {}),
            _MessageToDict(spark_job),
        )

        # Add Inputs
        if task_inputs is not None:
            task_inputs(self)

        # Container after the Inputs have been updated.
        self._container = self._get_container_definition(environment=environment)

    def _validate_inputs(self, inputs):
        """
        :param dict[Text, flytekit.models.interface.Variable] inputs:  Input variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        for k, v in _six.iteritems(inputs):
            sdk_type = _helpers.get_sdk_type_from_literal_type(v.type)
            if sdk_type not in input_types_supported:
                raise _user_exceptions.FlyteValidationException(
                    "Input Type '{}' not supported.  Only Primitives are supported for Scala/Java Spark.".format(
                        sdk_type
                    )
                )
        super(SdkGenericSparkTask, self)._validate_inputs(inputs)

    @_exception_scopes.system_entry_point
    def add_inputs(self, inputs):
        """
        Adds the inputs to this task.  This can be called multiple times, but it will fail if an input with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] inputs: names and variables
        """
        self._validate_inputs(inputs)
        self.interface.inputs.update(inputs)

    def _get_container_definition(
        self,
        environment=None,
    ):
        """
        :rtype: Container
        """

        args = []
        for k, v in _six.iteritems(self.interface.inputs):
            args.append("--{}".format(k))
            args.append("{{{{.Inputs.{}}}}}".format(k))

        return _task_models.Container(
            image=_internal_config.IMAGE.get(),
            command=[],
            args=args,
            resources=_task_models.Resources([], []),
            env=environment,
            config={},
        )

import typing

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec

import copy as _copy
import hashlib as _hashlib
import json as _json
import os as _os
import sys as _sys

import six as _six
from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit.bin import entrypoint as _entrypoint
from flytekit.common import constants as _constants
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import output as _task_output
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models
from flytekit.plugins import pyspark as _pyspark


class GlobalSparkContext(object):
    _SPARK_CONTEXT = None
    _SPARK_SESSION = None

    @classmethod
    def get_spark_context(cls):
        return cls._SPARK_CONTEXT

    @classmethod
    def get_spark_session(cls):
        return cls._SPARK_SESSION

    def __enter__(self):
        GlobalSparkContext._SPARK_CONTEXT = _pyspark.SparkContext()
        GlobalSparkContext._SPARK_SESSION = _pyspark.sql.SparkSession.builder.appName(
            "Flyte Spark SQL Context"
        ).getOrCreate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        GlobalSparkContext._SPARK_CONTEXT.stop()
        GlobalSparkContext._SPARK_CONTEXT = None
        return False


class SdkRunnableSparkContainer(_sdk_runnable.SdkRunnableContainer):
    @property
    def args(self):
        """
        Override args to remove the injection of command prefixes
        :rtype: list[Text]
        """
        return self._args


class SdkSparkTask(_sdk_runnable.SdkRunnableTask):
    """
    This class includes the additional logic for building a task that executes as a Spark Job.

    """

    def __init__(
        self,
        task_function,
        task_type,
        discovery_version,
        retries,
        interruptible,
        deprecated,
        discoverable,
        timeout,
        spark_type,
        spark_conf,
        hadoop_conf,
        environment,
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param bool interruptible: Whether or not task is interruptible
        :param Text deprecated:
        :param bool discoverable:
        :param datetime.timedelta timeout:
        :param dict[Text,Text] spark_conf:
        :param dict[Text,Text] hadoop_conf:
        :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.
        """

        spark_exec_path = _os.path.abspath(_entrypoint.__file__)
        if spark_exec_path.endswith(".pyc"):
            spark_exec_path = spark_exec_path[:-1]

        self._spark_job = _task_models.SparkJob(
            spark_conf=spark_conf,
            hadoop_conf=hadoop_conf,
            application_file="local://" + spark_exec_path,
            executor_path=_sys.executable,
            main_class="",
            spark_type=spark_type,
        )
        super(SdkSparkTask, self).__init__(
            task_function,
            task_type,
            discovery_version,
            retries,
            interruptible,
            deprecated,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            discoverable,
            timeout,
            environment,
            _MessageToDict(self._spark_job.to_flyte_idl()),
        )

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text,flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(
            inputs,
            {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)},
        )
        outputs_dict = {
            name: _task_output.OutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }

        inputs_dict.update(outputs_dict)

        with GlobalSparkContext():
            _exception_scopes.user_entry_point(self.task_function)(
                _sdk_runnable.ExecutionParameters(
                    execution_date=context.execution_date,
                    tmp_dir=context.working_directory,
                    stats=context.stats,
                    execution_id=context.execution_id,
                    logging=context.logging,
                ),
                GlobalSparkContext.get_spark_context(),
                **inputs_dict
            )
        return {
            _constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(
                literals={k: v.sdk_value for k, v in _six.iteritems(outputs_dict)}
            )
        }

    @property
    def spark_conf(self):
        return self._spark_job.spark_conf

    @property
    def hadoop_conf(self):
        return self._spark_job.hadoop_conf

    def _get_container_definition(self, **kwargs):
        """
        :rtype: SdkRunnableSparkContainer
        """
        return super(SdkSparkTask, self)._get_container_definition(cls=SdkRunnableSparkContainer, **kwargs)

    def _get_kwarg_inputs(self):
        # Trim off first two parameters as they are reserved for workflow_parameters and spark_context
        return set(_getargspec(self.task_function).args[2:])

    def with_overrides(
        self, new_spark_conf: typing.Dict[str, str] = None, new_hadoop_conf: typing.Dict[str, str] = None
    ):
        """
        Creates a new SparkJob instance with the modified configuration or timeouts
        """
        tk = _copy.deepcopy(self)
        tk._spark_job = self._spark_job.with_overrides(new_spark_conf, new_hadoop_conf)
        tk._custom = _MessageToDict(tk._spark_job.to_flyte_idl())

        salt = _hashlib.md5(_json.dumps(tk.custom, sort_keys=True).encode("utf-8")).hexdigest()
        tk._id._name = "{}-{}".format(self._id.name, salt)
        # We are overriding the platform name creation to prevent problems in dynamic
        tk.assign_name(tk._id._name)

        return tk

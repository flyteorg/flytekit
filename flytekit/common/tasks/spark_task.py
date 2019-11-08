from __future__ import absolute_import

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec

import os as _os
import sys as _sys
from flytekit.bin import entrypoint as _entrypoint
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks.mixins.executable_traits import function as _function_mixin, notebook as _notebook_mixin
from flytekit.models import task as _task_models
from flytekit.plugins import pyspark as _pyspark
from google.protobuf.json_format import MessageToDict as _MessageToDict


class GlobalSparkContext(object):
    _SPARK_CONTEXT = None

    @classmethod
    def get_spark_context(cls):
        return cls._SPARK_CONTEXT

    def __enter__(self):
        GlobalSparkContext._SPARK_CONTEXT = _pyspark.SparkContext()
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


class _SdkSparkTask(_sdk_runnable.SdkRunnableTask):
    """
    This class includes the additional logic for building a task that executes as a Spark Job.

    """
    def __init__(
            self,
            spark_conf=None,
            hadoop_conf=None,
            **kwargs
    ):
        """
        :param dict[Text,Text] spark_conf:
        :param dict[Text,Text] hadoop_conf:
        :param kwargs: See _sdk_runnable.SdkRunnableTask
        """
        spark_exec_path = _os.path.abspath(_entrypoint.__file__)
        if spark_exec_path.endswith('.pyc'):
            spark_exec_path = spark_exec_path[:-1]

        spark_job = _task_models.SparkJob(
            spark_conf=spark_conf,
            hadoop_conf=hadoop_conf,
            application_file="local://" + spark_exec_path,
            executor_path=_sys.executable,
        ).to_flyte_idl()
        super(_SdkSparkTask, self).__init__(
            custom=_MessageToDict(spark_job),
            **kwargs,
        )

    def _get_vargs(self, *args, **kwargs):
        """
        :param context:
        :rtype: list[T]
        """
        vargs = super(_SdkSparkTask, self)._get_vargs()
        vargs.append(GlobalSparkContext.get_spark_context())
        return vargs

    @_exception_scopes.system_entry_point
    def execute(self, *args, **kwargs):
        with GlobalSparkContext():
            return super(_SdkSparkTask, self).execute(*args, **kwargs)

    def _get_container_definition(
            self,
            **kwargs
    ):
        """
        :rtype: SdkRunnableSparkContainer
        """
        return super(_SdkSparkTask, self)._get_container_definition(cls=SdkRunnableSparkContainer, **kwargs)

    def _get_kwarg_inputs(self):
        # Trim off first two parameters as they are reserved for workflow_parameters and spark_context
        return set(_getargspec(self.task_function).args[2:])


class SparkFunctionTask(_function_mixin.WrappedFunctionTask, _SdkSparkTask):
    pass


class SparkNotebookTask(_notebook_mixin.NotebookTask, _SdkSparkTask):
    pass

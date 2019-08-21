from __future__ import absolute_import
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.engines import loader as _engine_loader
from flytekit.models.admin import task_execution as _task_execution_model
from flytekit.models.core import execution as _execution_models
import six as _six


class SdkTaskExecution(
    _six.with_metaclass(
        _sdk_bases.ExtendedSdkType,
        _task_execution_model.TaskExecution,
        _artifact_mixin.ExecutionArtifact,
    )
):
    def __init__(self, *args, **kwargs):
        super(SdkTaskExecution, self).__init__(*args, **kwargs)
        self._inputs = None
        self._outputs = None

    @property
    def is_complete(self):
        """
        Dictates whether or not the execution is complete.
        :rtype: bool
        """
        return self.closure.phase in {
            _execution_models.TaskExecutionPhase.ABORTED,
            _execution_models.TaskExecutionPhase.FAILED,
            _execution_models.TaskExecutionPhase.SUCCEEDED,
        }

    @property
    def inputs(self):
        """
        Returns the inputs of the task execution in the standard Python format that is produced by
        the type engine.
        :rtype: dict[Text, T]
        """
        if self._inputs is None:
            self._inputs = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _engine_loader.get_engine().get_task_execution(self).get_inputs()
            )
        return self._inputs

    @property
    def outputs(self):
        """
        Returns the outputs of the task execution, if available, in the standard Python format that is produced by
        the type engine. If not available, perhaps due to execution being in progress or an error being produced,
        this will raise an exception.
        :rtype: dict[Text, T]
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion("Please what until the task execution has completed before "
                                                  "requesting the outputs.")
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            self._outputs = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _engine_loader.get_engine().get_task_execution(self).get_outputs()
            )
        return self._outputs

    @property
    def error(self):
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        :rtype: flytekit.models.core.execution.ExecutionError or None
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion("Please what until the task execution has completed before "
                                                  "requesting error information.")
        return self.closure.error

    def get_child_executions(self, filters=None):
        """
        :param list[flytekit.models.filters.Filter] filters:
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        from flytekit.common import nodes as _nodes
        if not self.is_parent:
            raise _user_exceptions.FlyteAssertion("Only task executions marked with 'is_parent' have child executions.")
        models = _engine_loader.get_engine().get_task_execution(self).get_child_executions(filters=filters)
        return {
            k: _nodes.SdkNodeExecution.promote_from_model(v)
            for k, v in _six.iteritems(models)
        }

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param _task_execution_model.TaskExecution base_model:
        :rtype: SdkTaskExecution
        """
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            input_uri=base_model.input_uri,
            is_parent=base_model.is_parent,
        )

    def sync(self):
        _engine_loader.get_engine().get_task_execution(self).sync()

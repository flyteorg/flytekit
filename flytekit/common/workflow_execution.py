from __future__ import absolute_import

import six as _six
from flytekit.common import sdk_bases as _sdk_bases, nodes as _nodes
from flytekit.common.core import identifier as _core_identifier
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact
from flytekit.common.types import helpers as _type_helpers
from flytekit.engines import loader as _engine_loader
from flytekit.models import execution as _execution_models
from flytekit.models.core import execution as _core_execution_models


class SdkWorkflowExecution(
    _six.with_metaclass(
        _sdk_bases.ExtendedSdkType,
        _execution_models.Execution,
        _artifact.ExecutionArtifact
    )
):
    def __init__(self, *args, **kwargs):
        super(SdkWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def node_executions(self):
        """
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        return self._node_executions or {}

    @property
    def inputs(self):
        """
        Returns the inputs to the execution in the standard Python format as dictated by the type engine.
        :rtype:  dict[Text, T]
        """
        if self._inputs is None:
            self._inputs = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _engine_loader.get_engine().get_workflow_execution(self).get_inputs()
            )
        return self._inputs

    @property
    def outputs(self):
        """
        Returns the outputs to the execution in the standard Python format as dictated by the type engine.  If the
        execution ended in error or the execution is in progress, an exception will be raised.
        :rtype:  dict[Text, T] or None
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion("Please what until the node execution has completed before "
                                                  "requesting the outputs.")
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            self._outputs = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _engine_loader.get_engine().get_workflow_execution(self).get_outputs()
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
            raise _user_exceptions.FlyteAssertion("Please wait until a workflow has completed before checking for an "
                                                  "error.")
        return self.closure.error

    @property
    def is_complete(self):
        """
        Dictates whether or not the execution is complete.
        :rtype: bool
        """
        return self.closure.phase in {
            _core_execution_models.WorkflowExecutionPhase.ABORTED,
            _core_execution_models.WorkflowExecutionPhase.FAILED,
            _core_execution_models.WorkflowExecutionPhase.SUCCEEDED,
            _core_execution_models.WorkflowExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param _execution_models.Execution base_model:
        :rtype: SdkWorkflowExecution
        """
        return cls(
            closure=base_model.closure,
            id=_core_identifier.WorkflowExecutionIdentifier.promote_from_model(base_model.id),
            spec=base_model.spec,
        )

    @classmethod
    def fetch(cls, project, domain, name):
        """
        :param Text project:
        :param Text domain:
        :param Text name:
        :rtype: SdkWorkflowExecution
        """
        return cls.promote_from_model(
            _engine_loader.get_engine().fetch_workflow_execution(
                _core_identifier.WorkflowExecutionIdentifier(
                    project=project,
                    domain=domain,
                    name=name
                )
            )
        )

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        if not self.is_complete or self._node_executions is None:
            _engine_loader.get_engine().get_workflow_execution(self).sync()
            self._node_executions = self.get_node_executions()

    def get_node_executions(self, filters=None):
        """
        :param list[flytekit.models.filters.Filter] filters:
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        models = _engine_loader.get_engine().get_workflow_execution(self).get_node_executions(filters=filters)
        return {
            k: _nodes.SdkNodeExecution.promote_from_model(v)
            for k, v in _six.iteritems(models)
        }

    def terminate(self, cause):
        """
        :param Text cause:
        """
        _engine_loader.get_engine().get_workflow_execution(self).terminate(cause)

from typing import Any, Dict, List

from flytekit.clients.helpers import iterate_node_executions as _iterate_node_executions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import execution as _execution_models
from flytekit.models import filters as _filter_models
from flytekit.models.core import execution as _core_execution_models
from flytekit.remote import identifier as _core_identifier
from flytekit.remote import nodes as _nodes


class FlyteWorkflowExecution(_execution_models.Execution, _artifact.ExecutionArtifact):
    """A class encapsulating a workflow execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def node_executions(self) -> Dict[str, _nodes.FlyteNodeExecution]:
        """Get a dictionary of node executions that are a part of this workflow execution."""
        return self._node_executions or {}

    @property
    def inputs(self) -> Dict[str, Any]:
        """
        Returns the inputs to the execution in the standard python format as dictated by the type engine.
        """
        return self._inputs

    @property
    def outputs(self) -> Dict[str, Any]:
        """
        Returns the outputs to the execution in the standard python format as dictated by the type engine.

        :raises: ``FlyteAssertion`` error if execution is in progress or execution ended in error.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until the node execution has completed before requesting the outputs."
            )
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")
        return self._outputs

    @property
    def error(self) -> _core_execution_models.ExecutionError:
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until a workflow has completed before checking for an error."
            )
        return self.closure.error

    @property
    def is_complete(self) -> bool:
        """
        Whether or not the execution is complete.
        """
        return self.closure.phase in {
            _core_execution_models.WorkflowExecutionPhase.ABORTED,
            _core_execution_models.WorkflowExecutionPhase.FAILED,
            _core_execution_models.WorkflowExecutionPhase.SUCCEEDED,
            _core_execution_models.WorkflowExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: _execution_models.Execution) -> "FlyteWorkflowExecution":
        return cls(
            closure=base_model.closure,
            id=_core_identifier.WorkflowExecutionIdentifier.promote_from_model(base_model.id),
            spec=base_model.spec,
        )

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        if not self.is_complete or self._node_executions is None:
            self._sync_closure()
            self._node_executions = self.get_node_executions()

        for node_execution in self._node_executions.values():
            node_execution.sync()

    def _sync_closure(self):
        if not self.is_complete:
            client = _flyte_engine.get_client()
            self._closure = client.get_execution(self.id).closure

    def get_node_executions(self, filters: List[_filter_models.Filter] = None) -> Dict[str, _nodes.FlyteNodeExecution]:
        client = _flyte_engine.get_client()
        return {
            node.id.node_id: _nodes.FlyteNodeExecution.promote_from_model(node)
            for node in _iterate_node_executions(client, self.id, filters=filters)
        }

    def terminate(self, cause: str):
        _flyte_engine.get_client().terminate_execution(self.id, cause)

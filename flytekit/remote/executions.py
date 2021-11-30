from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.exceptions import user as user_exceptions
from flytekit.core.type_engine import LiteralsResolver
from flytekit.models import execution as execution_models
from flytekit.models import node_execution as node_execution_models
from flytekit.models.admin import task_execution as admin_task_execution_models
from flytekit.models.core import execution as core_execution_models
from flytekit.remote.workflow import FlyteWorkflow


class FlyteTaskExecution(admin_task_execution_models.TaskExecution):
    """A class encapsulating a task execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteTaskExecution, self).__init__(*args, **kwargs)
        self._inputs = None
        self._outputs = None

    @property
    def is_complete(self) -> bool:
        """Whether or not the execution is complete."""
        return self.closure.phase in {
            core_execution_models.TaskExecutionPhase.ABORTED,
            core_execution_models.TaskExecutionPhase.FAILED,
            core_execution_models.TaskExecutionPhase.SUCCEEDED,
        }

    @property
    def inputs(self) -> Dict[str, Any]:
        """
        Returns the inputs of the task execution in the standard Python format that is produced by
        the type engine.
        """
        return self._inputs

    @property
    def outputs(self) -> Dict[str, Any]:
        """
        Returns the outputs of the task execution, if available, in the standard Python format that is produced by
        the type engine.

        :raises: ``FlyteAssertion`` error if execution is in progress or execution ended in error.
        """
        if not self.is_complete:
            raise user_exceptions.FlyteAssertion(
                "Please wait until the node execution has completed before requesting the outputs."
            )
        if self.error:
            raise user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")
        return self._outputs

    @property
    def error(self) -> Optional[core_execution_models.ExecutionError]:
        """
        If execution is in progress, raise an exception. Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise user_exceptions.FlyteAssertion(
                "Please what until the task execution has completed before requesting error information."
            )
        return self.closure.error

    @classmethod
    def promote_from_model(cls, base_model: admin_task_execution_models.TaskExecution) -> "FlyteTaskExecution":
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            input_uri=base_model.input_uri,
            is_parent=base_model.is_parent,
        )


class FlyteWorkflowExecution(execution_models.Execution):
    """A class encapsulating a workflow execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._inputs = None
        self._outputs = None
        self._flyte_workflow: Optional[FlyteWorkflow] = None
        self._raw_inputs: Optional[LiteralsResolver] = None
        self._raw_outputs: Optional[LiteralsResolver] = None

    @property
    def node_executions(self) -> Dict[str, "FlyteNodeExecution"]:
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
    def raw_outputs(self) -> LiteralsResolver:
        if self._raw_outputs is None:
            raise ValueError(f"WF execution: {self} doesn't have raw outputs set")
        return self._raw_outputs

    @property
    def raw_inputs(self) -> LiteralsResolver:
        if self._raw_inputs is None:
            raise ValueError(f"WF execution: {self} doesn't have raw inputs set")
        return self._raw_inputs

    @property
    def error(self) -> core_execution_models.ExecutionError:
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
            core_execution_models.WorkflowExecutionPhase.ABORTED,
            core_execution_models.WorkflowExecutionPhase.FAILED,
            core_execution_models.WorkflowExecutionPhase.SUCCEEDED,
            core_execution_models.WorkflowExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: execution_models.Execution) -> "FlyteWorkflowExecution":
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            spec=base_model.spec,
        )


class FlyteNodeExecution(node_execution_models.NodeExecution):
    """A class encapsulating a node execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteNodeExecution, self).__init__(*args, **kwargs)
        self._task_executions = None
        self._workflow_executions = []
        self._underlying_node_executions = None
        self._inputs = None
        self._outputs = None
        self._interface = None

    @property
    def task_executions(self) -> List[FlyteTaskExecution]:
        return self._task_executions or []

    @property
    def workflow_executions(self) -> List[FlyteWorkflowExecution]:
        return self._workflow_executions

    @property
    def subworkflow_node_executions(self) -> Dict[str, FlyteNodeExecution]:
        """
        This returns underlying node executions in instances where the current node execution is
        a parent node. This happens when it's either a static or dynamic subworkflow.
        """
        return (
            {}
            if self._underlying_node_executions is None
            else {n.id.node_id: n for n in self._underlying_node_executions}
        )

    @property
    def executions(self) -> List[Union[FlyteTaskExecution, FlyteWorkflowExecution]]:
        return self.task_executions or self._underlying_node_executions or []

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
    def error(self) -> core_execution_models.ExecutionError:
        """
        If execution is in progress, raise an exception. Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until the node execution has completed before requesting error information."
            )
        return self.closure.error

    @property
    def is_complete(self) -> bool:
        """Whether or not the execution is complete."""
        return self.closure.phase in {
            core_execution_models.NodeExecutionPhase.ABORTED,
            core_execution_models.NodeExecutionPhase.FAILED,
            core_execution_models.NodeExecutionPhase.SKIPPED,
            core_execution_models.NodeExecutionPhase.SUCCEEDED,
            core_execution_models.NodeExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: node_execution_models.NodeExecution) -> "FlyteNodeExecution":
        return cls(
            closure=base_model.closure, id=base_model.id, input_uri=base_model.input_uri, metadata=base_model.metadata
        )

    @property
    def interface(self) -> "flytekit.remote.interface.TypedInterface":
        """
        Return the interface of the task or subworkflow associated with this node execution.
        """
        return self._interface

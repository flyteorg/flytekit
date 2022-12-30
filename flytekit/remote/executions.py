from __future__ import annotations

from abc import abstractmethod
from typing import Dict, List, Optional, Union

from flyteidl.admin import execution_pb2, node_execution_pb2, task_execution_pb2
from flyteidl.core import execution_pb2 as core_execution_pb2

from flytekit.core.type_engine import LiteralsResolver
from flytekit.exceptions import user as user_exceptions
from flytekit.remote.entities import FlyteTask, FlyteWorkflow


class RemoteExecutionBase(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._inputs: Optional[LiteralsResolver] = None
        self._outputs: Optional[LiteralsResolver] = None

    @property
    def inputs(self) -> Optional[LiteralsResolver]:
        return self._inputs

    @property
    @abstractmethod
    def error(self) -> core_execution_pb2.ExecutionError:
        ...

    @property
    @abstractmethod
    def is_done(self) -> bool:
        ...

    @property
    def outputs(self) -> Optional[LiteralsResolver]:
        """
        :return: Returns the outputs LiteralsResolver to the execution
        :raises: ``FlyteAssertion`` error if execution is in progress or execution ended in error.
        """
        if not self.is_done:
            raise user_exceptions.FlyteAssertion(
                "Please wait until the execution has completed before requesting the outputs."
            )
        if self.error:
            raise user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        return self._outputs


# TODO: inherited from task_execution_pb2.TaskExecution
class FlyteTaskExecution(RemoteExecutionBase):
    """A class encapsulating a task execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteTaskExecution, self).__init__(*args, **kwargs)
        self._flyte_task = None

    @property
    def task(self) -> Optional[FlyteTask]:
        return self._flyte_task

    @property
    def is_done(self) -> bool:
        """Whether or not the execution is complete."""
        return self.closure.phase in {
            core_execution_pb2.TaskExecution.ABORTED,
            core_execution_pb2.TaskExecution.FAILED,
            core_execution_pb2.TaskExecution.SUCCEEDED,
        }

    @property
    def error(self) -> Optional[core_execution_pb2.ExecutionError]:
        """
        If execution is in progress, raise an exception. Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_done:
            raise user_exceptions.FlyteAssertion(
                "Please what until the task execution has completed before requesting error information."
            )
        return self.closure.error

    @classmethod
    def promote_from_model(cls, base_model: task_execution_pb2.TaskExecution) -> "FlyteTaskExecution":
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            input_uri=base_model.input_uri,
            is_parent=base_model.is_parent,
        )


# TODO: inherited from execution_pb2.Execution
class FlyteWorkflowExecution(RemoteExecutionBase):
    """A class encapsulating a workflow execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._flyte_workflow: Optional[FlyteWorkflow] = None

    @property
    def flyte_workflow(self) -> Optional[FlyteWorkflow]:
        return self._flyte_workflow

    @property
    def node_executions(self) -> Dict[str, "FlyteNodeExecution"]:
        """Get a dictionary of node executions that are a part of this workflow execution."""
        return self._node_executions or {}

    @property
    def error(self) -> core_execution_pb2.ExecutionError:
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_done:
            raise user_exceptions.FlyteAssertion(
                "Please wait until a workflow has completed before checking for an error."
            )
        return self.closure.error

    @property
    def is_done(self) -> bool:
        """
        Whether or not the execution is complete.
        """
        return self.closure.phase in {
            core_execution_pb2.WorkflowExecution.ABORTED,
            core_execution_pb2.WorkflowExecution.FAILED,
            core_execution_pb2.WorkflowExecution.SUCCEEDED,
            core_execution_pb2.WorkflowExecution.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: execution_pb2.Execution) -> "FlyteWorkflowExecution":
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            spec=base_model.spec,
        )


# TODO: inherited from node_execution_pb2.NodeExecution
class FlyteNodeExecution(RemoteExecutionBase):
    """A class encapsulating a node execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteNodeExecution, self).__init__(*args, **kwargs)
        self._task_executions = None
        self._workflow_executions = []
        self._underlying_node_executions = None
        self._interface = None
        self._flyte_node = None

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
    def error(self) -> core_execution_pb2.ExecutionError:
        """
        If execution is in progress, raise an exception. Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_done:
            raise user_exceptions.FlyteAssertion(
                "Please wait until the node execution has completed before requesting error information."
            )
        return self.closure.error

    @property
    def is_done(self) -> bool:
        """Whether or not the execution is complete."""
        return self.closure.phase in {
            core_execution_pb2.NodeExecution.ABORTED,
            core_execution_pb2.NodeExecution.FAILED,
            core_execution_pb2.NodeExecution.SKIPPED,
            core_execution_pb2.NodeExecution.SUCCEEDED,
            core_execution_pb2.NodeExecution.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: node_execution_pb2.NodeExecution) -> "FlyteNodeExecution":
        return cls(
            closure=base_model.closure, id=base_model.id, input_uri=base_model.input_uri, metadata=base_model.metadata
        )

    # TODO: why this return type?
    @property
    def interface(self) -> "flytekit.remote.interface.TypedInterface":
        """
        Return the interface of the task or subworkflow associated with this node execution.
        """
        return self._interface

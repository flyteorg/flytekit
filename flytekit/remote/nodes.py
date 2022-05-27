import logging as _logging
from typing import Any, Dict, List, Optional, Union

import flytekit
from flytekit.clients.helpers import iterate_node_executions, iterate_task_executions
from flytekit.common import constants as _constants
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.utils import _dnsify
from flytekit.core.promise import NodeOutput
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import launch_plan as _launch_plan_model
from flytekit.models import node_execution as _node_execution_models
from flytekit.models import task as _task_model
from flytekit.models.core import execution as _execution_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.remote import component_nodes as _component_nodes
from flytekit.remote import identifier as _identifier
from flytekit.remote.tasks.executions import FlyteTaskExecution


class FlyteNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node):
    """A class encapsulating a remote Flyte node."""

    def __init__(
        self,
        id,
        upstream_nodes,
        bindings,
        metadata,
        flyte_task: Optional["FlyteTask"] = None,
        flyte_workflow: Optional["FlyteWorkflow"] = None,
        flyte_launch_plan: Optional["FlyteLaunchPlan"] = None,
        flyte_branch=None,
        parameter_mapping=True,
    ):
        non_none_entities = list(filter(None, [flyte_task, flyte_workflow, flyte_launch_plan, flyte_branch]))
        if len(non_none_entities) != 1:
            raise _user_exceptions.FlyteAssertion(
                "An Flyte node must have one underlying entity specified at once.  Received the following "
                "entities: {}".format(non_none_entities)
            )
        self._flyte_entity = flyte_task or flyte_workflow or flyte_launch_plan or flyte_branch

        workflow_node = None
        if flyte_workflow is not None:
            workflow_node = _component_nodes.FlyteWorkflowNode(flyte_workflow=flyte_workflow)
        elif flyte_launch_plan is not None:
            workflow_node = _component_nodes.FlyteWorkflowNode(flyte_launch_plan=flyte_launch_plan)

        super(FlyteNode, self).__init__(
            id=_dnsify(id) if id else None,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=_component_nodes.FlyteTaskNode(flyte_task) if flyte_task else None,
            workflow_node=workflow_node,
            branch_node=flyte_branch,
        )
        self._upstream = upstream_nodes

    @property
    def flyte_entity(self) -> Union["FlyteTask", "FlyteWorkflow", "FlyteLaunchPlan"]:
        return self._flyte_entity

    @classmethod
    def promote_from_model(
        cls,
        model: _workflow_model.Node,
        sub_workflows: Optional[Dict[_identifier.Identifier, _workflow_model.WorkflowTemplate]],
        node_launch_plans: Optional[Dict[_identifier.Identifier, _launch_plan_model.LaunchPlanSpec]],
        tasks: Optional[Dict[_identifier.Identifier, _task_model.TaskTemplate]],
    ) -> "FlyteNode":
        id = model.id
        if id in {_constants.START_NODE_ID, _constants.END_NODE_ID}:
            _logging.warning(f"Should not call promote from model on a start node or end node {model}")
            return None

        flyte_task_node, flyte_workflow_node = None, None
        if model.task_node is not None:
            flyte_task_node = _component_nodes.FlyteTaskNode.promote_from_model(model.task_node, tasks)
        elif model.workflow_node is not None:
            flyte_workflow_node = _component_nodes.FlyteWorkflowNode.promote_from_model(
                model.workflow_node,
                sub_workflows,
                node_launch_plans,
                tasks,
            )
        # TODO: Implement branch node https://github.com/flyteorg/flyte/issues/1116
        else:
            raise _system_exceptions.FlyteSystemException(
                f"Bad Node model, neither task nor workflow detected, node: {model}"
            )

        # When WorkflowTemplate models (containing node models) are returned by Admin, they've been compiled with a
        # start node. In order to make the promoted FlyteWorkflow look the same, we strip the start-node text back out.
        for model_input in model.inputs:
            if (
                model_input.binding.promise is not None
                and model_input.binding.promise.node_id == _constants.START_NODE_ID
            ):
                model_input.binding.promise._node_id = _constants.GLOBAL_INPUT_NODE_ID

        if flyte_task_node is not None:
            return cls(
                id=id,
                upstream_nodes=[],  # set downstream, model doesn't contain this information
                bindings=model.inputs,
                metadata=model.metadata,
                flyte_task=flyte_task_node.flyte_task,
            )
        elif flyte_workflow_node is not None:
            if flyte_workflow_node.flyte_workflow is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    flyte_workflow=flyte_workflow_node.flyte_workflow,
                )
            elif flyte_workflow_node.flyte_launch_plan is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    flyte_launch_plan=flyte_workflow_node.flyte_launch_plan,
                )
            raise _system_exceptions.FlyteSystemException(
                "Bad FlyteWorkflowNode model, both launch plan and workflow are None"
            )
        raise _system_exceptions.FlyteSystemException("Bad FlyteNode model, both task and workflow nodes are empty")

    @property
    def upstream_nodes(self) -> List["FlyteNode"]:
        return self._upstream

    @property
    def upstream_node_ids(self) -> List[str]:
        return list(sorted(n.id for n in self.upstream_nodes))

    @property
    def outputs(self) -> Dict[str, NodeOutput]:
        return self._outputs

    def assign_id_and_return(self, id: str):
        if self.id:
            raise _user_exceptions.FlyteAssertion(
                f"Error assigning ID: {id} because {self} is already assigned. Has this node been ssigned to another "
                "workflow already?"
            )
        self._id = _dnsify(id) if id else None
        self._metadata.name = id
        return self

    def with_overrides(self, *args, **kwargs):
        # TODO: Implement overrides
        raise NotImplementedError("Overrides are not supported in Flyte yet.")

    def __repr__(self) -> str:
        return f"Node(ID: {self.id})"


class FlyteNodeExecution(_node_execution_models.NodeExecution, _artifact_mixin.ExecutionArtifact):
    """A class encapsulating a node execution being run on a Flyte remote backend."""

    def __init__(self, *args, **kwargs):
        super(FlyteNodeExecution, self).__init__(*args, **kwargs)
        self._task_executions = None
        self._subworkflow_node_executions = None
        self._inputs = None
        self._outputs = None
        self._interface = None

    @property
    def task_executions(self) -> List["flytekit.remote.tasks.executions.FlyteTaskExecution"]:
        return self._task_executions or []

    @property
    def subworkflow_node_executions(self) -> Dict[str, "flytekit.remote.nodes.FlyteNodeExecution"]:
        return (
            {}
            if self._subworkflow_node_executions is None
            else {n.id.node_id: n for n in self._subworkflow_node_executions}
        )

    @property
    def executions(self) -> List[_artifact_mixin.ExecutionArtifact]:
        return self.task_executions or self._subworkflow_node_executions or []

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
    def error(self) -> _execution_models.ExecutionError:
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
            _execution_models.NodeExecutionPhase.ABORTED,
            _execution_models.NodeExecutionPhase.FAILED,
            _execution_models.NodeExecutionPhase.SKIPPED,
            _execution_models.NodeExecutionPhase.SUCCEEDED,
            _execution_models.NodeExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: _node_execution_models.NodeExecution) -> "FlyteNodeExecution":
        return cls(
            closure=base_model.closure, id=base_model.id, input_uri=base_model.input_uri, metadata=base_model.metadata
        )

    @property
    def interface(self) -> "flytekit.remote.interface.TypedInterface":
        """
        Return the interface of the task or subworkflow associated with this node execution.
        """
        return self._interface

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        if self.metadata.is_parent_node:
            if not self.is_complete or self._subworkflow_node_executions is None:
                self._subworkflow_node_executions = [
                    FlyteNodeExecution.promote_from_model(n)
                    for n in iterate_node_executions(
                        _flyte_engine.get_client(),
                        workflow_execution_identifier=self.id.execution_id,
                        unique_parent_id=self.id.node_id,
                    )
                ]
        else:
            if not self.is_complete or self._task_executions is None:
                self._task_executions = [
                    FlyteTaskExecution.promote_from_model(t)
                    for t in iterate_task_executions(_flyte_engine.get_client(), self.id)
                ]

        self._sync_closure()
        for execution in self.executions:
            execution.sync()

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        """
        self._closure = _flyte_engine.get_client().get_node_execution(self.id).closure

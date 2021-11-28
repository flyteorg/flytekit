from __future__ import annotations

import logging as _logging
from typing import Dict, List, Optional, Union

from flytekit.common import constants as _constants
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.core.promise import NodeOutput
from flytekit.models import launch_plan as _launch_plan_model
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as id_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.remote import component_nodes as _component_nodes


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

        task_node = None
        if flyte_task:
            task_node = _component_nodes.FlyteTaskNode(flyte_task)
        branch_node = None

        super(FlyteNode, self).__init__(
            id=id,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=task_node,
            workflow_node=workflow_node,
            branch_node=branch_node,
        )
        self._upstream = upstream_nodes

    @property
    def flyte_entity(self) -> Union["FlyteTask", "FlyteWorkflow", "FlyteLaunchPlan"]:
        return self._flyte_entity

    @classmethod
    def promote_from_model(
        cls,
        model: _workflow_model.Node,
        sub_workflows: Optional[Dict[id_models.Identifier, _workflow_model.WorkflowTemplate]],
        node_launch_plans: Optional[Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec]],
        tasks: Optional[Dict[id_models.Identifier, _task_model.TaskTemplate]],
    ) -> FlyteNode:
        node_model_id = model.id
        # TODO: Consider removing
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
        # TODO: Consider removing
        for model_input in model.inputs:
            if (
                model_input.binding.promise is not None
                and model_input.binding.promise.node_id == _constants.START_NODE_ID
            ):
                model_input.binding.promise._node_id = _constants.GLOBAL_INPUT_NODE_ID

        if flyte_task_node is not None:
            return cls(
                id=node_model_id,
                upstream_nodes=[],  # set downstream, model doesn't contain this information
                bindings=model.inputs,
                metadata=model.metadata,
                flyte_task=flyte_task_node.flyte_task,
            )
        elif flyte_workflow_node is not None:
            if flyte_workflow_node.flyte_workflow is not None:
                return cls(
                    id=node_model_id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    flyte_workflow=flyte_workflow_node.flyte_workflow,
                )
            elif flyte_workflow_node.flyte_launch_plan is not None:
                return cls(
                    id=node_model_id,
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
    def upstream_nodes(self) -> List[FlyteNode]:
        return self._upstream

    @property
    def upstream_node_ids(self) -> List[str]:
        return list(sorted(n.id for n in self.upstream_nodes))

    @property
    def outputs(self) -> Dict[str, NodeOutput]:
        return self._outputs

    def __repr__(self) -> str:
        return f"Node(ID: {self.id})"

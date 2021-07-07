from typing import Dict, List, Optional

from flytekit.common import constants as _constants
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.models import task as _task_models
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core import workflow as _workflow_models
from flytekit.remote import identifier as _identifier
from flytekit.remote import interface as _interfaces
from flytekit.remote import nodes as _nodes


class FlyteWorkflow(_hash_mixin.HashOnReferenceMixin, _workflow_models.WorkflowTemplate):
    """A Flyte control plane construct."""

    def __init__(
        self,
        nodes: List[_nodes.FlyteNode],
        interface,
        output_bindings,
        id,
        metadata,
        metadata_defaults,
    ):
        for node in nodes:
            for upstream in node.upstream_nodes:
                if upstream.id is None:
                    raise _user_exceptions.FlyteAssertion(
                        "Some nodes contained in the workflow were not found in the workflow description.  Please "
                        "ensure all nodes are either assigned to attributes within the class or an element in a "
                        "list, dict, or tuple which is stored as an attribute in the class."
                    )
        super(FlyteWorkflow, self).__init__(
            id=id,
            metadata=metadata,
            metadata_defaults=metadata_defaults,
            interface=interface,
            nodes=nodes,
            outputs=output_bindings,
        )
        self._flyte_nodes = nodes

    @property
    def upstream_entities(self):
        return set(n.executable_flyte_object for n in self._flyte_nodes)

    @property
    def interface(self) -> _interfaces.TypedInterface:
        return super(FlyteWorkflow, self).interface

    @property
    def entity_type_text(self) -> str:
        return "Workflow"

    @property
    def resource_type(self):
        return _identifier_model.ResourceType.WORKFLOW

    def get_sub_workflows(self) -> List["FlyteWorkflow"]:
        result = []
        for node in self.nodes:
            if node.workflow_node is not None and node.workflow_node.sub_workflow_ref is not None:
                if (
                    node.executable_flyte_object is not None
                    and node.executable_flyte_object.entity_type_text == "Workflow"
                ):
                    result.append(node.executable_flyte_object)
                    result.extend(node.executable_flyte_object.get_sub_workflows())
                else:
                    raise _system_exceptions.FlyteSystemException(
                        "workflow node with subworkflow found but bad executable "
                        "object {}".format(node.executable_flyte_object)
                    )

            # get subworkflows in conditional branches
            if node.branch_node is not None:
                if_else: _workflow_models.IfElseBlock = node.branch_node.if_else
                leaf_nodes: List[_nodes.FlyteNode] = filter(
                    None,
                    [
                        if_else.case.then_node,
                        *([] if if_else.other is None else [x.then_node for x in if_else.other]),
                        if_else.else_node,
                    ],
                )
                for leaf_node in leaf_nodes:
                    exec_flyte_obj = leaf_node.executable_flyte_object
                    if exec_flyte_obj is not None and exec_flyte_obj.entity_type_text == "Workflow":
                        result.append(exec_flyte_obj)
                        result.extend(exec_flyte_obj.get_sub_workflows())

        return result

    @classmethod
    def get_non_system_nodes(cls, nodes: List[_workflow_models.Node]) -> List[_workflow_models.Node]:
        return [n for n in nodes if n.id not in {_constants.START_NODE_ID, _constants.END_NODE_ID}]

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_models.WorkflowTemplate,
        sub_workflows: Optional[Dict[_identifier.Identifier, _workflow_models.WorkflowTemplate]] = None,
        tasks: Optional[Dict[_identifier.Identifier, _task_models.TaskTemplate]] = None,
    ) -> "FlyteWorkflow":
        base_model_non_system_nodes = cls.get_non_system_nodes(base_model.nodes)
        sub_workflows = sub_workflows or {}
        tasks = tasks or {}
        node_map = {
            n.id: _nodes.FlyteNode.promote_from_model(n, sub_workflows, tasks) for n in base_model_non_system_nodes
        }

        # Set upstream nodes for each node
        for n in base_model_non_system_nodes:
            current = node_map[n.id]
            for upstream_id in current.upstream_node_ids:
                upstream_node = node_map[upstream_id]
                current << upstream_node

        # No inputs/outputs specified, see the constructor for more information on the overrides.
        return cls(
            nodes=list(node_map.values()),
            id=_identifier.Identifier.promote_from_model(base_model.id),
            metadata=base_model.metadata,
            metadata_defaults=base_model.metadata_defaults,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            output_bindings=base_model.outputs,
        )

    def __call__(self, *args, **input_map):
        raise NotImplementedError

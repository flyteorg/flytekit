from __future__ import annotations

from typing import Dict, List, Optional

from flytekit.core import constants as _constants
from flytekit.core import hash as _hash_mixin
from flytekit.exceptions import user as _user_exceptions
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import task as _task_models
from flytekit.models.core import compiler as compiler_models
from flytekit.models.core import identifier as id_models
from flytekit.models.core import workflow as _workflow_models
from flytekit.remote import interface as _interfaces
from flytekit.remote import nodes as _nodes
from flytekit.remote.remote_callable import RemoteEntity


class FlyteWorkflow(_hash_mixin.HashOnReferenceMixin, RemoteEntity, _workflow_models.WorkflowTemplate):
    """A class encapsulating a remote Flyte workflow."""

    def __init__(
        self,
        id: id_models.Identifier,
        nodes: List[_nodes.FlyteNode],
        interface,
        output_bindings,
        metadata,
        metadata_defaults,
        subworkflows: Optional[Dict[id_models.Identifier, _workflow_models.WorkflowTemplate]] = None,
        tasks: Optional[Dict[id_models.Identifier, _task_models.TaskTemplate]] = None,
        launch_plans: Optional[Dict[id_models.Identifier, launch_plan_models.LaunchPlanSpec]] = None,
        compiled_closure: Optional[compiler_models.CompiledWorkflowClosure] = None,
    ):
        # TODO: Remove check
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

        # Optional things that we save for ease of access when promoting from a model or CompiledWorkflowClosure
        self._subworkflows = subworkflows
        self._tasks = tasks
        self._launch_plans = launch_plans
        self._compiled_closure = compiled_closure
        self._node_map = None
        self._name = id.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def sub_workflows(self) -> Optional[Dict[id_models.Identifier, _workflow_models.WorkflowTemplate]]:
        return self._subworkflows

    @property
    def entity_type_text(self) -> str:
        return "Workflow"

    @property
    def resource_type(self):
        return id_models.ResourceType.WORKFLOW

    @property
    def flyte_nodes(self) -> List[_nodes.FlyteNode]:
        return self._flyte_nodes

    @classmethod
    def get_non_system_nodes(cls, nodes: List[_workflow_models.Node]) -> List[_workflow_models.Node]:
        return [n for n in nodes if n.id not in {_constants.START_NODE_ID, _constants.END_NODE_ID}]

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_models.WorkflowTemplate,
        sub_workflows: Optional[Dict[id_models, _workflow_models.WorkflowTemplate]] = None,
        node_launch_plans: Optional[Dict[id_models, launch_plan_models.LaunchPlanSpec]] = None,
        tasks: Optional[Dict[id_models, _task_models.TaskTemplate]] = None,
    ) -> FlyteWorkflow:
        base_model_non_system_nodes = cls.get_non_system_nodes(base_model.nodes)
        sub_workflows = sub_workflows or {}
        tasks = tasks or {}
        node_map = {
            node.id: _nodes.FlyteNode.promote_from_model(node, sub_workflows, node_launch_plans, tasks)
            for node in base_model_non_system_nodes
        }

        # Set upstream nodes for each node
        for n in base_model_non_system_nodes:
            current = node_map[n.id]
            for upstream_id in n.upstream_node_ids:
                upstream_node = node_map[upstream_id]
                current._upstream.append(upstream_node)

        # No inputs/outputs specified, see the constructor for more information on the overrides.
        wf = cls(
            id=base_model.id,
            nodes=list(node_map.values()),
            metadata=base_model.metadata,
            metadata_defaults=base_model.metadata_defaults,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            output_bindings=base_model.outputs,
            subworkflows=sub_workflows,
            tasks=tasks,
            launch_plans=node_launch_plans,
        )

        wf._node_map = node_map

        return wf

    @classmethod
    def promote_from_closure(
        cls,
        closure: compiler_models.CompiledWorkflowClosure,
        node_launch_plans: Optional[Dict[id_models, launch_plan_models.LaunchPlanSpec]] = None,
    ):
        """
        Extracts out the relevant portions of a FlyteWorkflow from a closure from the control plane.

        :param closure: This is the closure returned by Admin
        :param node_launch_plans: The reason this exists is because the compiled closure doesn't have launch plans.
          It only has subworkflows and tasks. Why this is is unclear. If supplied, this map of launch plans will be
        :return:
        """
        sub_workflows = {sw.template.id: sw.template for sw in closure.sub_workflows}
        tasks = {t.template.id: t.template for t in closure.tasks}

        flyte_wf = FlyteWorkflow.promote_from_model(
            base_model=closure.primary.template,
            sub_workflows=sub_workflows,
            node_launch_plans=node_launch_plans,
            tasks=tasks,
        )
        flyte_wf._compiled_closure = closure
        return flyte_wf

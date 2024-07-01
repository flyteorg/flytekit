"""
This module contains shadow entities for all Flyte entities as represented in Flyte Admin / Control Plane.
The goal is to enable easy access, manipulation of these entities.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union

from flytekit import FlyteContext
from flytekit.core import constants as _constants
from flytekit.core import hash as _hash_mixin
from flytekit.core import hash as hash_mixin
from flytekit.core.promise import create_and_link_node_from_remote
from flytekit.exceptions import system as _system_exceptions
from flytekit.exceptions import user as _user_exceptions
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import launch_plan as _launch_plan_model
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import task as _task_model
from flytekit.models import task as _task_models
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.core import compiler as compiler_models
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core import identifier as id_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.core import workflow as _workflow_models
from flytekit.models.core.identifier import Identifier
from flytekit.models.core.workflow import Node, WorkflowMetadata, WorkflowMetadataDefaults
from flytekit.models.interface import TypedInterface
from flytekit.models.literals import Binding
from flytekit.models.task import TaskSpec
from flytekit.remote import interface as _interface
from flytekit.remote import interface as _interfaces
from flytekit.remote.remote_callable import RemoteEntity


class FlyteTask(hash_mixin.HashOnReferenceMixin, RemoteEntity, TaskSpec):
    """A class encapsulating a remote Flyte task."""

    def __init__(
        self,
        id,
        type,
        metadata,
        interface,
        custom,
        container=None,
        task_type_version: int = 0,
        config=None,
        should_register: bool = False,
    ):
        super(FlyteTask, self).__init__(
            template=_task_model.TaskTemplate(
                id,
                type,
                metadata,
                interface,
                custom,
                container=container,
                task_type_version=task_type_version,
                config=config,
            )
        )
        self._should_register = should_register

    @property
    def id(self):
        """
        This is generated by the system and uniquely identifies the task.

        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self.template.id

    @property
    def type(self):
        """
        This is used to identify additional extensions for use by Propeller or SDK.

        :rtype: Text
        """
        return self.template.type

    @property
    def metadata(self):
        """
        This contains information needed at runtime to determine behavior such as whether or not outputs are
        discoverable, timeouts, and retries.

        :rtype: TaskMetadata
        """
        return self.template.metadata

    @property
    def interface(self):
        """
        The interface definition for this task.

        :rtype: flytekit.models.interface.TypedInterface
        """
        return self.template.interface

    @property
    def custom(self):
        """
        Arbitrary dictionary containing metadata for custom plugins.

        :rtype: dict[Text, T]
        """
        return self.template.custom

    @property
    def task_type_version(self):
        return self.template.task_type_version

    @property
    def container(self):
        """
        If not None, the target of execution should be a container.

        :rtype: Container
        """
        return self.template.container

    @property
    def config(self):
        """
        Arbitrary dictionary containing metadata for parsing and handling custom plugins.

        :rtype: dict[Text, T]
        """
        return self.template.config

    @property
    def security_context(self):
        return self.template.security_context

    @property
    def k8s_pod(self):
        return self.template.k8s_pod

    @property
    def sql(self):
        return self.template.sql

    @property
    def should_register(self) -> bool:
        return self._should_register

    @property
    def name(self) -> str:
        return self.template.id.name

    @property
    def resource_type(self) -> _identifier_model.ResourceType:
        return _identifier_model.ResourceType.TASK

    @property
    def entity_type_text(self) -> str:
        return "Task"

    @classmethod
    def promote_from_model(cls, base_model: _task_model.TaskTemplate) -> FlyteTask:
        t = cls(
            id=base_model.id,
            type=base_model.type,
            metadata=base_model.metadata,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            custom=base_model.custom,
            container=base_model.container,
            task_type_version=base_model.task_type_version,
        )
        # Override the newly generated name if one exists in the base model
        if not base_model.id.is_empty:
            t._id = base_model.id

        return t


class FlyteTaskNode(_workflow_model.TaskNode):
    """A class encapsulating a task that a Flyte node needs to execute."""

    def __init__(self, flyte_task: FlyteTask):
        super(FlyteTaskNode, self).__init__(None)
        self._flyte_task = flyte_task

    @property
    def reference_id(self) -> id_models.Identifier:
        """A globally unique identifier for the task."""
        return self._flyte_task.id

    @property
    def flyte_task(self) -> FlyteTask:
        return self._flyte_task

    @classmethod
    def promote_from_model(cls, task: FlyteTask) -> FlyteTaskNode:
        """
        Takes the idl wrapper for a TaskNode,
        and returns the hydrated Flytekit object for it by fetching it with the FlyteTask control plane.
        """
        return cls(flyte_task=task)


class FlyteWorkflowNode(_workflow_model.WorkflowNode):
    """A class encapsulating a workflow that a Flyte node needs to execute."""

    def __init__(
        self,
        flyte_workflow: FlyteWorkflow = None,
        flyte_launch_plan: FlyteLaunchPlan = None,
    ):
        if flyte_workflow and flyte_launch_plan:
            raise _system_exceptions.FlyteSystemException(
                "FlyteWorkflowNode cannot be called with both a workflow and a launchplan specified, please pick "
                f"one. workflow: {flyte_workflow} launchPlan: {flyte_launch_plan}",
            )

        self._flyte_workflow = flyte_workflow
        self._flyte_launch_plan = flyte_launch_plan
        super(FlyteWorkflowNode, self).__init__(
            launchplan_ref=self._flyte_launch_plan.id if self._flyte_launch_plan else None,
            sub_workflow_ref=self._flyte_workflow.id if self._flyte_workflow else None,
        )

    def __repr__(self) -> str:
        if self.flyte_workflow is not None:
            return f"FlyteWorkflowNode with workflow: {self.flyte_workflow}"
        return f"FlyteWorkflowNode with launch plan: {self.flyte_launch_plan}"

    @property
    def launchplan_ref(self) -> id_models.Identifier:
        """A globally unique identifier for the launch plan, which should map to Admin."""
        return self._flyte_launch_plan.id if self._flyte_launch_plan else None

    @property
    def sub_workflow_ref(self):
        return self._flyte_workflow.id if self._flyte_workflow else None

    @property
    def flyte_launch_plan(self) -> FlyteLaunchPlan:
        return self._flyte_launch_plan

    @property
    def flyte_workflow(self) -> FlyteWorkflow:
        return self._flyte_workflow

    @classmethod
    def _promote_workflow(
        cls,
        wf: _workflow_models.WorkflowTemplate,
        sub_workflows: Optional[Dict[Identifier, _workflow_models.WorkflowTemplate]] = None,
        tasks: Optional[Dict[Identifier, FlyteTask]] = None,
        node_launch_plans: Optional[Dict[Identifier, launch_plan_models.LaunchPlanSpec]] = None,
    ) -> FlyteWorkflow:
        return FlyteWorkflow.promote_from_model(
            wf,
            sub_workflows=sub_workflows,
            node_launch_plans=node_launch_plans,
            tasks=tasks,
        )

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_model.WorkflowNode,
        sub_workflows: Dict[id_models.Identifier, _workflow_model.WorkflowTemplate],
        node_launch_plans: Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec],
        tasks: Dict[Identifier, FlyteTask],
        converted_sub_workflows: Dict[id_models.Identifier, FlyteWorkflow],
    ) -> Tuple[FlyteWorkflowNode, Dict[id_models.Identifier, FlyteWorkflow]]:
        if base_model.launchplan_ref is not None:
            return (
                cls(
                    flyte_launch_plan=FlyteLaunchPlan.promote_from_model(
                        base_model.launchplan_ref, node_launch_plans[base_model.launchplan_ref]
                    )
                ),
                converted_sub_workflows,
            )
        elif base_model.sub_workflow_ref is not None:
            # the workflow templates for sub-workflows should have been included in the original response
            if base_model.reference in sub_workflows:
                wf = None
                if base_model.reference not in converted_sub_workflows:
                    wf = cls._promote_workflow(
                        sub_workflows[base_model.reference],
                        sub_workflows=sub_workflows,
                        node_launch_plans=node_launch_plans,
                        tasks=tasks,
                    )
                    converted_sub_workflows[base_model.reference] = wf
                else:
                    wf = converted_sub_workflows[base_model.reference]
                return cls(flyte_workflow=wf), converted_sub_workflows
            raise _system_exceptions.FlyteSystemException(f"Subworkflow {base_model.reference} not found.")

        raise _system_exceptions.FlyteSystemException(
            "Bad workflow node model, neither subworkflow nor launchplan specified."
        )


class FlyteBranchNode(_workflow_model.BranchNode):
    def __init__(self, if_else: _workflow_model.IfElseBlock):
        super().__init__(if_else)

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_model.BranchNode,
        sub_workflows: Dict[id_models.Identifier, _workflow_model.WorkflowTemplate],
        node_launch_plans: Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec],
        tasks: Dict[id_models.Identifier, FlyteTask],
        converted_sub_workflows: Dict[id_models.Identifier, FlyteWorkflow],
    ) -> Tuple[FlyteBranchNode, Dict[id_models.Identifier, FlyteWorkflow]]:
        block = base_model.if_else
        block.case._then_node, converted_sub_workflows = FlyteNode.promote_from_model(
            block.case.then_node,
            sub_workflows,
            node_launch_plans,
            tasks,
            converted_sub_workflows,
        )

        for o in block.other:
            o._then_node, converted_sub_workflows = FlyteNode.promote_from_model(
                o.then_node, sub_workflows, node_launch_plans, tasks, converted_sub_workflows
            )

        else_node = None
        if block.else_node:
            else_node, converted_sub_workflows = FlyteNode.promote_from_model(
                block.else_node, sub_workflows, node_launch_plans, tasks, converted_sub_workflows
            )

        new_if_else_block = _workflow_model.IfElseBlock(block.case, block.other, else_node, block.error)

        return cls(new_if_else_block), converted_sub_workflows


class FlyteGateNode(_workflow_model.GateNode):
    @classmethod
    def promote_from_model(cls, model: _workflow_model.GateNode):
        return cls(model.signal, model.sleep, model.approve)


class FlyteArrayNode(_workflow_model.ArrayNode):
    @classmethod
    def promote_from_model(cls, model: _workflow_model.ArrayNode):
        return cls(
            node=model._node,
            parallelism=model._parallelism,
            min_successes=model._min_successes,
            min_success_ratio=model._min_success_ratio,
        )


class FlyteNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node):
    """A class encapsulating a remote Flyte node."""

    def __init__(
        self,
        id,
        upstream_nodes,
        bindings,
        metadata,
        task_node: Optional[FlyteTaskNode] = None,
        workflow_node: Optional[FlyteWorkflowNode] = None,
        branch_node: Optional[FlyteBranchNode] = None,
        gate_node: Optional[FlyteGateNode] = None,
        array_node: Optional[FlyteArrayNode] = None,
    ):
        if not task_node and not workflow_node and not branch_node and not gate_node and not array_node:
            raise _user_exceptions.FlyteAssertion(
                "An Flyte node must have one of task|workflow|branch|gate|array entity specified at once"
            )
        # TODO: Revisit flyte_branch_node and flyte_gate_node, should they be another type like Condition instead
        #       of a node?
        self._flyte_task_node = task_node
        if task_node:
            self._flyte_entity = task_node.flyte_task
        elif workflow_node:
            self._flyte_entity = workflow_node.flyte_workflow or workflow_node.flyte_launch_plan
        else:
            self._flyte_entity = branch_node or gate_node or array_node

        super(FlyteNode, self).__init__(
            id=id,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=task_node,
            workflow_node=workflow_node,
            branch_node=branch_node,
            gate_node=gate_node,
            array_node=array_node,
        )
        self._upstream = upstream_nodes

    @property
    def task_node(self) -> Optional[FlyteTaskNode]:
        return self._flyte_task_node

    @property
    def flyte_entity(self) -> Union[FlyteTask, FlyteWorkflow, FlyteLaunchPlan, FlyteBranchNode]:
        return self._flyte_entity

    @classmethod
    def _promote_task_node(cls, t: FlyteTask) -> FlyteTaskNode:
        return FlyteTaskNode.promote_from_model(t)

    @classmethod
    def _promote_workflow_node(
        cls,
        wn: _workflow_model.WorkflowNode,
        sub_workflows: Dict[id_models.Identifier, _workflow_model.WorkflowTemplate],
        node_launch_plans: Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec],
        tasks: Dict[Identifier, FlyteTask],
        converted_sub_workflows: Dict[id_models.Identifier, FlyteWorkflow],
    ) -> Tuple[FlyteWorkflowNode, Dict[id_models.Identifier, FlyteWorkflow]]:
        return FlyteWorkflowNode.promote_from_model(
            wn,
            sub_workflows,
            node_launch_plans,
            tasks,
            converted_sub_workflows,
        )

    @classmethod
    def promote_from_model(
        cls,
        model: _workflow_model.Node,
        sub_workflows: Optional[Dict[id_models.Identifier, _workflow_model.WorkflowTemplate]],
        node_launch_plans: Optional[Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec]],
        tasks: Dict[id_models.Identifier, FlyteTask],
        converted_sub_workflows: Dict[id_models.Identifier, FlyteWorkflow],
    ) -> Tuple[Optional[FlyteNode], Dict[id_models.Identifier, FlyteWorkflow]]:
        node_model_id = model.id
        # TODO: Consider removing
        if id in {_constants.START_NODE_ID, _constants.END_NODE_ID}:
            logger.warning(f"Should not call promote from model on a start node or end node {model}")
            return None, converted_sub_workflows

        flyte_task_node, flyte_workflow_node, flyte_branch_node, flyte_gate_node, flyte_array_node = (
            None,
            None,
            None,
            None,
            None,
        )
        if model.task_node is not None:
            if model.task_node.reference_id not in tasks:
                raise RuntimeError(
                    f"Remote Workflow closure does not have task with id {model.task_node.reference_id}."
                )
            flyte_task_node = cls._promote_task_node(tasks[model.task_node.reference_id])
        elif model.workflow_node is not None:
            flyte_workflow_node, converted_sub_workflows = cls._promote_workflow_node(
                model.workflow_node,
                sub_workflows,
                node_launch_plans,
                tasks,
                converted_sub_workflows,
            )
        elif model.branch_node is not None:
            flyte_branch_node, converted_sub_workflows = FlyteBranchNode.promote_from_model(
                model.branch_node,
                sub_workflows,
                node_launch_plans,
                tasks,
                converted_sub_workflows,
            )
        elif model.gate_node is not None:
            flyte_gate_node = FlyteGateNode.promote_from_model(model.gate_node)
        elif model.array_node is not None:
            flyte_array_node = FlyteArrayNode.promote_from_model(model.array_node)
            # TODO: validate task in tasks
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

        return (
            cls(
                id=node_model_id,
                upstream_nodes=[],  # set downstream, model doesn't contain this information
                bindings=model.inputs,
                metadata=model.metadata,
                task_node=flyte_task_node,
                workflow_node=flyte_workflow_node,
                branch_node=flyte_branch_node,
                gate_node=flyte_gate_node,
                array_node=flyte_array_node,
            ),
            converted_sub_workflows,
        )

    @property
    def upstream_nodes(self) -> List[FlyteNode]:
        return self._upstream

    @property
    def upstream_node_ids(self) -> List[str]:
        return list(sorted(n.id for n in self.upstream_nodes))

    def __repr__(self) -> str:
        return f"Node(ID: {self.id})"


class FlyteWorkflow(_hash_mixin.HashOnReferenceMixin, RemoteEntity, WorkflowSpec):
    """A class encapsulating a remote Flyte workflow."""

    def __init__(
        self,
        id: id_models.Identifier,
        nodes: List[FlyteNode],
        interface,
        output_bindings,
        metadata,
        metadata_defaults,
        subworkflows: Optional[List[FlyteWorkflow]] = None,
        tasks: Optional[List[FlyteTask]] = None,
        launch_plans: Optional[Dict[id_models.Identifier, launch_plan_models.LaunchPlanSpec]] = None,
        compiled_closure: Optional[compiler_models.CompiledWorkflowClosure] = None,
        should_register: bool = False,
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

        self._flyte_sub_workflows = subworkflows
        template_subworkflows = []
        if subworkflows:
            template_subworkflows = [swf.template for swf in subworkflows]

        super(FlyteWorkflow, self).__init__(
            template=_workflow_models.WorkflowTemplate(
                id=id,
                metadata=metadata,
                metadata_defaults=metadata_defaults,
                interface=interface,
                nodes=nodes,
                outputs=output_bindings,
            ),
            sub_workflows=template_subworkflows,
        )
        self._flyte_nodes = nodes

        # Optional things that we save for ease of access when promoting from a model or CompiledWorkflowClosure
        self._tasks = tasks
        self._launch_plans = launch_plans
        self._compiled_closure = compiled_closure
        self._node_map = None
        self._name = id.name
        self._should_register = should_register

    @property
    def name(self) -> str:
        return self._name

    @property
    def flyte_tasks(self) -> Optional[List[FlyteTask]]:
        return self._tasks

    @property
    def should_register(self) -> bool:
        return self._should_register

    @property
    def flyte_sub_workflows(self) -> List[FlyteWorkflow]:
        return self._flyte_sub_workflows

    @property
    def entity_type_text(self) -> str:
        return "Workflow"

    @property
    def resource_type(self):
        return id_models.ResourceType.WORKFLOW

    @property
    def flyte_nodes(self) -> List[FlyteNode]:
        return self._flyte_nodes

    @property
    def id(self) -> Identifier:
        """
        This is an autogenerated id by the system. The id is globally unique across Flyte.
        """
        return self.template.id

    @property
    def metadata(self) -> WorkflowMetadata:
        """
        This contains information on how to run the workflow.
        """
        return self.template.metadata

    @property
    def metadata_defaults(self) -> WorkflowMetadataDefaults:
        """
        This contains information on how to run the workflow.
        :rtype: WorkflowMetadataDefaults
        """
        return self.template.metadata_defaults

    @property
    def interface(self) -> TypedInterface:
        """
        Defines a strongly typed interface for the Workflow (inputs, outputs). This can include some optional
        parameters.
        """
        return self.template.interface

    @property
    def nodes(self) -> List[Node]:
        """
        A list of nodes. In addition, "globals" is a special reserved node id that can be used to consume
        workflow inputs
        """
        return self.template.nodes

    @property
    def outputs(self) -> List[Binding]:
        """
        A list of output bindings that specify how to construct workflow outputs. Bindings can
        pull node outputs or specify literals. All workflow outputs specified in the interface field must be bound
        in order for the workflow to be validated. A workflow has an implicit dependency on all of its nodes
        to execute successfully in order to bind final outputs.
        """
        return self.template.outputs

    @property
    def failure_node(self) -> Node:
        """
        Node failure_node: A catch-all node. This node is executed whenever the execution engine determines the
        workflow has failed. The interface of this node must match the Workflow interface with an additional input
        named "error" of type pb.lyft.flyte.core.Error.
        """
        return self.template.failure_node

    @classmethod
    def get_non_system_nodes(cls, nodes: List[_workflow_models.Node]) -> List[_workflow_models.Node]:
        return [n for n in nodes if n.id not in {_constants.START_NODE_ID, _constants.END_NODE_ID}]

    @classmethod
    def _promote_node(
        cls,
        model: _workflow_model.Node,
        sub_workflows: Optional[Dict[id_models.Identifier, _workflow_model.WorkflowTemplate]],
        node_launch_plans: Optional[Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec]],
        tasks: Dict[id_models.Identifier, FlyteTask],
        converted_sub_workflows: Dict[id_models.Identifier, FlyteWorkflow],
    ) -> Tuple[Optional[FlyteNode], Dict[id_models.Identifier, FlyteWorkflow]]:
        return FlyteNode.promote_from_model(model, sub_workflows, node_launch_plans, tasks, converted_sub_workflows)

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_models.WorkflowTemplate,
        sub_workflows: Optional[Dict[Identifier, _workflow_models.WorkflowTemplate]] = None,
        tasks: Optional[Dict[Identifier, FlyteTask]] = None,
        node_launch_plans: Optional[Dict[Identifier, launch_plan_models.LaunchPlanSpec]] = None,
    ) -> FlyteWorkflow:
        base_model_non_system_nodes = cls.get_non_system_nodes(base_model.nodes)

        node_map = {}
        converted_sub_workflows = {}
        for node in base_model_non_system_nodes:
            flyte_node, converted_sub_workflows = cls._promote_node(
                node, sub_workflows, node_launch_plans, tasks, converted_sub_workflows
            )
            node_map[node.id] = flyte_node

        # Set upstream nodes for each node
        for n in base_model_non_system_nodes:
            current = node_map[n.id]
            for upstream_id in n.upstream_node_ids:
                upstream_node = node_map[upstream_id]
                current._upstream.append(upstream_node)

        subworkflow_list = []
        if converted_sub_workflows:
            subworkflow_list = [v for _, v in converted_sub_workflows.items()]

        task_list = []
        if tasks:
            task_list = [t for _, t in tasks.items()]

        # No inputs/outputs specified, see the constructor for more information on the overrides.
        wf = cls(
            id=base_model.id,
            nodes=list(node_map.values()),
            metadata=base_model.metadata,
            metadata_defaults=base_model.metadata_defaults,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            output_bindings=base_model.outputs,
            subworkflows=subworkflow_list,
            tasks=task_list,
            launch_plans=node_launch_plans,
        )

        wf._node_map = node_map

        return wf

    @classmethod
    def _promote_task(cls, t: _task_models.TaskTemplate) -> FlyteTask:
        return FlyteTask.promote_from_model(t)

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
            It only has subworkflows and tasks. Why this is unclear. If supplied, this map of launch plans will be
        """
        sub_workflows = {sw.template.id: sw.template for sw in closure.sub_workflows}
        tasks = {}
        if closure.tasks:
            tasks = {t.template.id: cls._promote_task(t.template) for t in closure.tasks}

        flyte_wf = cls.promote_from_model(
            base_model=closure.primary.template,
            sub_workflows=sub_workflows,
            node_launch_plans=node_launch_plans,
            tasks=tasks,
        )
        flyte_wf._compiled_closure = closure
        return flyte_wf


class FlyteLaunchPlan(hash_mixin.HashOnReferenceMixin, RemoteEntity, _launch_plan_models.LaunchPlanSpec):
    """A class encapsulating a remote Flyte launch plan."""

    def __init__(self, id, *args, **kwargs):
        super(FlyteLaunchPlan, self).__init__(*args, **kwargs)
        # Set all the attributes we expect this class to have
        self._id = id
        self._name = id.name

        # The interface is not set explicitly unless fetched in an engine context
        self._interface = None
        # If fetched when creating this object, can store it here.
        self._flyte_workflow = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def flyte_workflow(self) -> Optional[FlyteWorkflow]:
        return self._flyte_workflow

    @classmethod
    def promote_from_model(cls, id: id_models.Identifier, model: _launch_plan_models.LaunchPlanSpec) -> FlyteLaunchPlan:
        lp = cls(
            id=id,
            workflow_id=model.workflow_id,
            default_inputs=_interface_models.ParameterMap(model.default_inputs.parameters),
            fixed_inputs=model.fixed_inputs,
            entity_metadata=model.entity_metadata,
            labels=model.labels,
            annotations=model.annotations,
            auth_role=model.auth_role,
            raw_output_data_config=model.raw_output_data_config,
            max_parallelism=model.max_parallelism,
            security_context=model.security_context,
        )
        return lp

    @property
    def id(self) -> id_models.Identifier:
        return self._id

    @property
    def is_scheduled(self) -> bool:
        if self.entity_metadata.schedule.cron_expression:
            return True
        elif self.entity_metadata.schedule.rate and self.entity_metadata.schedule.rate.value:
            return True
        elif self.entity_metadata.schedule.cron_schedule and self.entity_metadata.schedule.cron_schedule.schedule:
            return True
        else:
            return False

    @property
    def workflow_id(self) -> id_models.Identifier:
        return self._workflow_id

    @property
    def interface(self) -> Optional[_interface.TypedInterface]:
        """
        The interface is not technically part of the admin.LaunchPlanSpec in the IDL, however the workflow ID is, and
        from the workflow ID, fetch will fill in the interface. This is nice because then you can __call__ the=
        object and get a node.
        """
        return self._interface

    @property
    def resource_type(self) -> id_models.ResourceType:
        return id_models.ResourceType.LAUNCH_PLAN

    @property
    def entity_type_text(self) -> str:
        return "Launch Plan"

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        fixed_input_lits = self.fixed_inputs.literals or {}
        default_input_params = self.default_inputs.parameters or {}
        return create_and_link_node_from_remote(
            ctx,
            entity=self,
            _inputs_not_allowed=set(fixed_input_lits.keys()),
            _ignorable_inputs=set(default_input_params.keys()),
            **kwargs,
        )  # noqa

    def __repr__(self) -> str:
        return f"FlyteLaunchPlan(ID: {self.id} Interface: {self.interface}) - Spec {super().__repr__()})"

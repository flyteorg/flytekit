import sys
import typing
from collections import OrderedDict
from typing import Callable, Dict, List, Optional, Tuple, Union

from flyteidl.admin import schedule_pb2

from flytekit import ImageSpec, PodTemplate, PythonFunctionTask, SourceCode
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core import context_manager
from flytekit.core.array_node import ArrayNode
from flytekit.core.array_node_map_task import ArrayNodeMapTask
from flytekit.core.base_task import PythonTask
from flytekit.core.condition import BranchNode
from flytekit.core.container_task import ContainerTask
from flytekit.core.gate import Gate
from flytekit.core.launch_plan import LaunchPlan, ReferenceLaunchPlan
from flytekit.core.legacy_map_task import MapPythonTask
from flytekit.core.node import Node
from flytekit.core.options import Options
from flytekit.core.python_auto_container import (
    PythonAutoContainerTask,
)
from flytekit.core.python_function_task import EagerAsyncPythonFunctionTask
from flytekit.core.reference_entity import ReferenceEntity, ReferenceSpec, ReferenceTemplate
from flytekit.core.task import ReferenceTask
from flytekit.core.utils import ClassDecorator, _dnsify, _serialize_pod_spec
from flytekit.core.workflow import ReferenceWorkflow, WorkflowBase
from flytekit.models import common as _common_models
from flytekit.models import interface as interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core import workflow as _core_wf
from flytekit.models.core import workflow as workflow_model
from flytekit.models.core.workflow import ApproveCondition, GateNode, SignalCondition, SleepCondition, TaskNodeOverrides
from flytekit.models.core.workflow import ArrayNode as ArrayNodeModel
from flytekit.models.core.workflow import BranchNode as BranchNodeModel
from flytekit.models.task import TaskSpec, TaskTemplate

FlyteLocalEntity = Union[
    PythonTask,
    BranchNode,
    Node,
    LaunchPlan,
    WorkflowBase,
    ReferenceWorkflow,
    ReferenceTask,
    ReferenceLaunchPlan,
    ReferenceEntity,
    ArrayNode,
]
FlyteControlPlaneEntity = Union[
    TaskSpec,
    _launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
    workflow_model.Node,
    BranchNodeModel,
    ArrayNodeModel,
]


def to_serializable_case(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    c: _core_wf.IfBlock,
    options: Optional[Options] = None,
) -> _core_wf.IfBlock:
    if c is None:
        raise ValueError("Cannot convert none cases to registrable")
    then_node = get_serializable(entity_mapping, settings, c.then_node, options=options)
    return _core_wf.IfBlock(condition=c.condition, then_node=then_node)


def to_serializable_cases(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    cases: List[_core_wf.IfBlock],
    options: Optional[Options] = None,
) -> Optional[List[_core_wf.IfBlock]]:
    if cases is None:
        return None
    ret_cases = []
    for c in cases:
        ret_cases.append(to_serializable_case(entity_mapping, settings, c, options))
    return ret_cases


def get_command_prefix_for_fast_execute(settings: SerializationSettings) -> List[str]:
    return [
        "pyflyte-fast-execute",
        "--additional-distribution",
        settings.fast_serialization_settings.distribution_location
        if settings.fast_serialization_settings and settings.fast_serialization_settings.distribution_location
        else "{{ .remote_package_path }}",
        "--dest-dir",
        settings.fast_serialization_settings.destination_dir
        if settings.fast_serialization_settings and settings.fast_serialization_settings.destination_dir
        else "{{ .dest_dir }}",
        "--",
    ]


def prefix_with_fast_execute(settings: SerializationSettings, cmd: typing.List[str]) -> List[str]:
    return get_command_prefix_for_fast_execute(settings) + cmd


def _fast_serialize_command_fn(
    settings: SerializationSettings, task: PythonAutoContainerTask
) -> Callable[[SerializationSettings], List[str]]:
    """
    This function is only applicable for Pod tasks.
    """
    default_command = task.get_default_command(settings)

    def fn(settings: SerializationSettings) -> List[str]:
        return prefix_with_fast_execute(settings, default_command)

    return fn


def get_serializable_task(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: FlyteLocalEntity,
    options: Optional[Options] = None,
) -> TaskSpec:
    task_id = _identifier_model.Identifier(
        _identifier_model.ResourceType.TASK,
        settings.project,
        settings.domain,
        entity.name,
        settings.version,
    )

    if isinstance(entity, PythonFunctionTask) and entity.execution_mode == PythonFunctionTask.ExecutionBehavior.DYNAMIC:
        for e in context_manager.FlyteEntities.entities:
            if isinstance(e, PythonAutoContainerTask):
                # 1. Build the ImageSpec for all the entities that are inside the current context,
                # 2. Add images to the serialization context, so the dynamic task can look it up at runtime.
                if isinstance(e.container_image, ImageSpec):
                    if settings.image_config.images is None:
                        settings.image_config = ImageConfig.create_from(settings.image_config.default_image)
                    settings.image_config.images.append(
                        Image.look_up_image_info(e.container_image.id, e.get_image(settings))
                    )

        # In case of Dynamic tasks, we want to pass the serialization context, so that they can reconstruct the state
        # from the serialization context. This is passed through an environment variable, that is read from
        # during dynamic serialization
        settings = settings.with_serialized_context()

        if entity.node_dependency_hints is not None:
            for entity_hint in entity.node_dependency_hints:
                get_serializable(entity_mapping, settings, entity_hint, options)

    if isinstance(entity, EagerAsyncPythonFunctionTask):
        settings = settings.with_serialized_context()

    container = entity.get_container(settings)
    # This pod will be incorrect when doing fast serialize
    pod = entity.get_k8s_pod(settings)

    if settings.should_fast_serialize():
        # This handles container tasks.
        if container and isinstance(entity, (PythonAutoContainerTask, MapPythonTask, ArrayNodeMapTask)):
            # For fast registration, we'll need to muck with the command, but on
            # ly for certain kinds of tasks. Specifically,
            # tasks that rely on user code defined in the container. This should be encapsulated by the auto container
            # parent class
            container._args = prefix_with_fast_execute(settings, container.args)

        # If the pod spec is not None, we have to get it again, because the one we retrieved above will be incorrect.
        # The reason we have to call get_k8s_pod again, instead of just modifying the command in this file, is because
        # the pod spec is a K8s library object, and we shouldn't be messing around with it in this file.
        elif pod and not isinstance(entity, ContainerTask):
            if isinstance(entity, (MapPythonTask, ArrayNodeMapTask)):
                entity.set_command_prefix(get_command_prefix_for_fast_execute(settings))
                pod = entity.get_k8s_pod(settings)
            else:
                entity.set_command_fn(_fast_serialize_command_fn(settings, entity))
                pod = entity.get_k8s_pod(settings)
                entity.reset_command_fn()

    entity_config = entity.get_config(settings) or {}
    extra_config = {}

    if hasattr(entity, "task_function"):
        if isinstance(entity.task_function, ClassDecorator):
            extra_config = entity.task_function.get_extra_config()
    if entity.enable_deck:
        entity.metadata.generates_deck = True

    merged_config = {**entity_config, **extra_config}

    tt = TaskTemplate(
        id=task_id,
        type=entity.task_type,
        metadata=entity.metadata.to_taskmetadata_model(),
        interface=entity.interface,
        custom=entity.get_custom(settings),
        container=container,
        task_type_version=entity.task_type_version,
        security_context=entity.security_context,
        config=merged_config,
        k8s_pod=pod,
        sql=entity.get_sql(settings),
        extended_resources=entity.get_extended_resources(settings),
    )
    if settings.should_fast_serialize() and isinstance(entity, PythonAutoContainerTask):
        entity.reset_command_fn()

    return TaskSpec(template=tt, docs=entity.docs)


def get_serializable_workflow(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: WorkflowBase,
    options: Optional[Options] = None,
) -> admin_workflow_models.WorkflowSpec:
    # Serialize all nodes
    serialized_nodes = []
    sub_wfs = []
    for n in entity.nodes:
        # Ignore start nodes
        if n.id == _common_constants.GLOBAL_INPUT_NODE_ID:
            continue

        # Recursively serialize the node
        serialized_nodes.append(get_serializable(entity_mapping, settings, n, options))

        # If the node is workflow Node or Branch node, we need to handle it specially, to extract all subworkflows,
        # so that they can be added to the workflow being serialized
        if isinstance(n.flyte_entity, WorkflowBase):
            # We are currently not supporting reference workflows since these will
            # require a network call to flyteadmin to populate the WorkflowTemplate
            # object
            if isinstance(n.flyte_entity, ReferenceWorkflow):
                raise ValueError(
                    "Reference sub-workflows are currently unsupported. Use reference launch plans instead."
                )
            sub_wf_spec = get_serializable(entity_mapping, settings, n.flyte_entity, options)
            if not isinstance(sub_wf_spec, admin_workflow_models.WorkflowSpec):
                raise TypeError(
                    f"Unexpected type for serialized form of workflow. Expected {admin_workflow_models.WorkflowSpec}, but got {type(sub_wf_spec)}"
                )
            sub_wfs.append(sub_wf_spec.template)
            sub_wfs.extend(sub_wf_spec.sub_workflows)

        from flytekit.remote import FlyteWorkflow

        if isinstance(n.flyte_entity, FlyteWorkflow):
            for swf in n.flyte_entity.flyte_sub_workflows:
                sub_wf = get_serializable(entity_mapping, settings, swf, options)
                sub_wfs.append(sub_wf.template)
            main_wf = get_serializable(entity_mapping, settings, n.flyte_entity, options)
            sub_wfs.append(main_wf.template)

        if isinstance(n.flyte_entity, BranchNode):
            if_else: workflow_model.IfElseBlock = n.flyte_entity._ifelse_block
            # See comment in get_serializable_branch_node also. Again this is a List[Node] even though it's supposed
            # to be a List[workflow_model.Node]
            leaf_nodes: List[Node] = filter(  # noqa
                None,
                [
                    if_else.case.then_node,
                    *([] if if_else.other is None else [x.then_node for x in if_else.other]),
                    if_else.else_node,
                ],
            )
            for leaf_node in leaf_nodes:
                if isinstance(leaf_node.flyte_entity, WorkflowBase):
                    sub_wf_spec = get_serializable(entity_mapping, settings, leaf_node.flyte_entity, options)
                    sub_wfs.append(sub_wf_spec.template)
                    sub_wfs.extend(sub_wf_spec.sub_workflows)
                elif isinstance(leaf_node.flyte_entity, FlyteWorkflow):
                    get_serializable(entity_mapping, settings, leaf_node.flyte_entity, options)
                    sub_wfs.append(leaf_node.flyte_entity)
                    sub_wfs.extend([s for s in leaf_node.flyte_entity.sub_workflows.values()])

    serialized_failure_node = None
    if entity.failure_node:
        serialized_failure_node = get_serializable(entity_mapping, settings, entity.failure_node, options)
        if isinstance(entity.failure_node.flyte_entity, WorkflowBase):
            sub_wf_spec = get_serializable(entity_mapping, settings, entity.failure_node.flyte_entity, options)
            sub_wfs.append(sub_wf_spec.template)
            sub_wfs.extend(sub_wf_spec.sub_workflows)

    wf_id = _identifier_model.Identifier(
        resource_type=_identifier_model.ResourceType.WORKFLOW,
        project=settings.project,
        domain=settings.domain,
        name=entity.name,
        version=settings.version,
    )

    wf_t = workflow_model.WorkflowTemplate(
        id=wf_id,
        metadata=entity.workflow_metadata.to_flyte_model(),
        metadata_defaults=entity.workflow_metadata_defaults.to_flyte_model(),
        interface=entity.interface,
        nodes=serialized_nodes,
        outputs=entity.output_bindings,
        failure_node=serialized_failure_node,
    )

    return admin_workflow_models.WorkflowSpec(
        template=wf_t, sub_workflows=sorted(set(sub_wfs), key=lambda x: x.short_string()), docs=entity.docs
    )


def get_serializable_launch_plan(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: LaunchPlan,
    recurse_downstream: bool = True,
    options: Optional[Options] = None,
) -> _launch_plan_models.LaunchPlan:
    """
    :param entity_mapping:
    :param settings:
    :param entity:
    :param options:
    :param recurse_downstream: This boolean indicate is wf for the entity should also be recursed to
    :return:
    """
    if recurse_downstream:
        wf_spec = get_serializable(entity_mapping, settings, entity.workflow, options)
        wf_id = wf_spec.template.id
    else:
        wf_id = _identifier_model.Identifier(
            resource_type=_identifier_model.ResourceType.WORKFLOW,
            project=settings.project,
            domain=settings.domain,
            name=entity.workflow.name,
            version=settings.version,
        )

    if not options:
        options = Options()

    if options and options.raw_output_data_config:
        raw_prefix_config = options.raw_output_data_config
    else:
        raw_prefix_config = entity.raw_output_data_config or _common_models.RawOutputDataConfig("")

    if entity.trigger:
        lc = entity.trigger.to_flyte_idl(entity)
        if isinstance(lc, schedule_pb2.Schedule):
            raise ValueError("Please continue to use the schedule arg, the trigger arg is not implemented yet")
    else:
        lc = None

    lps = _launch_plan_models.LaunchPlanSpec(
        workflow_id=wf_id,
        entity_metadata=_launch_plan_models.LaunchPlanMetadata(
            schedule=entity.schedule,
            notifications=options.notifications or entity.notifications,
            launch_conditions=lc,
        ),
        default_inputs=entity.parameters,
        fixed_inputs=entity.fixed_inputs,
        labels=options.labels or entity.labels or _common_models.Labels({}),
        annotations=options.annotations or entity.annotations or _common_models.Annotations({}),
        auth_role=None,
        raw_output_data_config=raw_prefix_config,
        max_parallelism=options.max_parallelism or entity.max_parallelism,
        security_context=options.security_context or entity.security_context,
        overwrite_cache=options.overwrite_cache or entity.overwrite_cache,
    )

    lp_id = _identifier_model.Identifier(
        resource_type=_identifier_model.ResourceType.LAUNCH_PLAN,
        project=settings.project,
        domain=settings.domain,
        name=entity.name,
        version=settings.version,
    )
    lp_model = _launch_plan_models.LaunchPlan(
        id=lp_id,
        spec=lps,
        closure=_launch_plan_models.LaunchPlanClosure(
            state=None,
            expected_inputs=interface_models.ParameterMap({}),
            expected_outputs=interface_models.VariableMap({}),
        ),
        auto_activate=entity.should_auto_activate,
    )

    return lp_model


def get_serializable_node(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: Node,
    options: Optional[Options] = None,
) -> workflow_model.Node:
    if entity.flyte_entity is None:
        raise ValueError(f"Node {entity.id} has no flyte entity")

    upstream_nodes = [
        get_serializable(entity_mapping, settings, n, options=options)
        for n in entity.upstream_nodes
        if n.id != _common_constants.GLOBAL_INPUT_NODE_ID
    ]

    # Reference entities also inherit from the classes in the second if statement so address them first.
    if isinstance(entity.flyte_entity, ReferenceEntity):
        ref_spec = get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
        ref_template = ref_spec.template
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
        )
        if ref_template.resource_type == _identifier_model.ResourceType.TASK:
            node_model._task_node = workflow_model.TaskNode(reference_id=ref_template.id)
        elif ref_template.resource_type == _identifier_model.ResourceType.WORKFLOW:
            node_model._workflow_node = workflow_model.WorkflowNode(sub_workflow_ref=ref_template.id)
        elif ref_template.resource_type == _identifier_model.ResourceType.LAUNCH_PLAN:
            node_model._workflow_node = workflow_model.WorkflowNode(launchplan_ref=ref_template.id)
        else:
            raise TypeError(
                f"Unexpected resource type for reference entity {entity.flyte_entity}: {ref_template.resource_type}"
            )
        return node_model

    from flytekit.remote import FlyteLaunchPlan, FlyteTask, FlyteWorkflow

    if isinstance(entity.flyte_entity, ArrayNode):
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            array_node=get_serializable_array_node(entity_mapping, settings, entity, options=options),
        )
    elif isinstance(entity.flyte_entity, ArrayNodeMapTask):
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            array_node=get_serializable_array_node_map_task(entity_mapping, settings, entity, options=options),
        )
        # TODO: do I need this?
        # if entity._aliases:
        #     node_model._output_aliases = entity._aliases
    elif isinstance(entity.flyte_entity, PythonTask):
        # handle pod template overrides
        override_pod_spec = {}
        if entity._pod_template is not None and settings.should_fast_serialize():
            entity.flyte_entity.set_command_fn(_fast_serialize_command_fn(settings, entity.flyte_entity))
            override_pod_spec = _serialize_pod_spec(
                entity._pod_template, entity.flyte_entity._get_container(settings), settings
            )
        task_spec = get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=workflow_model.TaskNode(
                reference_id=task_spec.template.id,
                overrides=TaskNodeOverrides(
                    resources=entity._resources,
                    extended_resources=entity._extended_resources,
                    container_image=entity._container_image,
                    pod_template=PodTemplate(
                        pod_spec=override_pod_spec,
                        labels=entity._pod_template.labels if entity._pod_template.labels else None,
                        annotations=entity._pod_template.annotations if entity._pod_template.annotations else None,
                        primary_container_name=entity._pod_template.primary_container_name
                        if entity._pod_template.primary_container_name
                        else None,
                    )
                    if entity._pod_template
                    else None,
                ),
            ),
        )
        if entity._aliases:
            node_model._output_aliases = entity._aliases

    elif isinstance(entity.flyte_entity, WorkflowBase):
        wf_spec = get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            workflow_node=workflow_model.WorkflowNode(sub_workflow_ref=wf_spec.template.id),
        )

    elif isinstance(entity.flyte_entity, BranchNode):
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            branch_node=get_serializable(entity_mapping, settings, entity.flyte_entity, options=options),
        )

    elif isinstance(entity.flyte_entity, LaunchPlan):
        lp_spec = get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)

        # Node's inputs should not contain the data which is fixed input
        node_input = []
        for b in entity.bindings:
            if b.var not in entity.flyte_entity.fixed_inputs.literals:
                node_input.append(b)

        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=node_input,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            workflow_node=workflow_model.WorkflowNode(launchplan_ref=lp_spec.id),
        )

    elif isinstance(entity.flyte_entity, Gate):
        if entity.flyte_entity.sleep_duration:
            gn = GateNode(sleep=SleepCondition(duration=entity.flyte_entity.sleep_duration))
        elif entity.flyte_entity.input_type:
            output_name = list(entity.flyte_entity.python_interface.outputs.keys())[0]  # should be o0
            gn = GateNode(
                signal=SignalCondition(
                    entity.flyte_entity.name, type=entity.flyte_entity.literal_type, output_variable_name=output_name
                )
            )
        else:
            gn = GateNode(approve=ApproveCondition(entity.flyte_entity.name))
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            gate_node=gn,
        )

    elif isinstance(entity.flyte_entity, FlyteTask):
        # Recursive call doesn't do anything except put the entity on the map.
        get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=workflow_model.TaskNode(
                reference_id=entity.flyte_entity.id,
                overrides=TaskNodeOverrides(
                    resources=entity._resources,
                    extended_resources=entity._extended_resources,
                    container_image=entity._container_image,
                ),
            ),
        )
    elif isinstance(entity.flyte_entity, FlyteWorkflow):
        wf_spec = get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
        for sub_wf in entity.flyte_entity.flyte_sub_workflows:
            get_serializable(entity_mapping, settings, sub_wf, options=options)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            workflow_node=workflow_model.WorkflowNode(sub_workflow_ref=wf_spec.id),
        )
    elif isinstance(entity.flyte_entity, FlyteLaunchPlan):
        # Recursive call doesn't do anything except put the entity on the map.
        get_serializable(entity_mapping, settings, entity.flyte_entity, options=options)
        # Node's inputs should not contain the data which is fixed input
        node_input = []
        for b in entity.bindings:
            if b.var not in entity.flyte_entity.fixed_inputs.literals:
                node_input.append(b)

        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=node_input,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            workflow_node=workflow_model.WorkflowNode(launchplan_ref=entity.flyte_entity.id),
        )
    else:
        raise ValueError(f"Node contained non-serializable entity {entity._flyte_entity}")

    return node_model


def get_serializable_array_node(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    node: FlyteLocalEntity,
    options: Optional[Options] = None,
) -> ArrayNodeModel:
    array_node = node.flyte_entity
    # pass in parent node metadata to be set for subnode
    array_node.metadata = node.metadata
    return ArrayNodeModel(
        node=get_serializable_node(entity_mapping, settings, array_node, options=options),
        parallelism=array_node.concurrency,
        min_successes=array_node.min_successes,
        min_success_ratio=array_node.min_success_ratio,
        execution_mode=array_node.execution_mode,
        is_original_sub_node_interface=array_node.is_original_sub_node_interface,
        data_mode=array_node.data_mode,
    )


def get_serializable_array_node_map_task(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    node: Node,
    options: Optional[Options] = None,
) -> ArrayNodeModel:
    # TODO Add support for other flyte entities
    entity = node.flyte_entity
    task_spec = get_serializable(entity_mapping, settings, entity, options)
    task_node = workflow_model.TaskNode(
        reference_id=task_spec.template.id,
        overrides=TaskNodeOverrides(
            resources=node._resources,
            extended_resources=node._extended_resources,
            container_image=node._container_image,
        ),
    )
    node = workflow_model.Node(
        id=entity.name,
        metadata=entity.sub_node_metadata,
        inputs=node.bindings,
        upstream_node_ids=[],
        output_aliases=[],
        task_node=task_node,
    )
    return ArrayNodeModel(
        node=node,
        parallelism=entity.concurrency,
        min_successes=entity.min_successes,
        min_success_ratio=entity.min_success_ratio,
        execution_mode=entity.execution_mode,
        is_original_sub_node_interface=entity.is_original_sub_node_interface,
    )


def get_serializable_branch_node(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: FlyteLocalEntity,
    options: Optional[Options] = None,
) -> BranchNodeModel:
    # We have to iterate through the blocks to convert the nodes from the internal Node type to the Node model type.
    # This was done to avoid having to create our own IfElseBlock object (i.e. condition.py just uses the model
    # directly) even though the node there is of the wrong type (our type instead of the model type).
    # TODO this should be cleaned up instead of mutation, we probably should just create a new object
    first = to_serializable_case(entity_mapping, settings, entity._ifelse_block.case, options)
    other = to_serializable_cases(entity_mapping, settings, entity._ifelse_block.other, options)
    else_node_model = None
    if entity._ifelse_block.else_node:
        else_node_model = get_serializable(entity_mapping, settings, entity._ifelse_block.else_node, options=options)

    return BranchNodeModel(
        if_else=_core_wf.IfElseBlock(
            case=first, other=other, else_node=else_node_model, error=entity._ifelse_block.error
        )
    )


def get_reference_spec(
    entity_mapping: OrderedDict, settings: SerializationSettings, entity: ReferenceEntity
) -> ReferenceSpec:
    template = ReferenceTemplate(entity.id, entity.reference.resource_type)
    return ReferenceSpec(template)


def get_serializable_flyte_workflow(
    entity: "FlyteWorkflow", settings: SerializationSettings
) -> FlyteControlPlaneEntity:
    """
    TODO replace with deep copy
    """

    def _mutate_task_node(tn: workflow_model.TaskNode):
        tn.reference_id._project = settings.project
        tn.reference_id._domain = settings.domain

    def _mutate_branch_node_task_ids(bn: workflow_model.BranchNode):
        _mutate_node(bn.if_else.case.then_node)
        for c in bn.if_else.other:
            _mutate_node(c.then_node)
        if bn.if_else.else_node:
            _mutate_node(bn.if_else.else_node)

    def _mutate_workflow_node(wn: workflow_model.WorkflowNode):
        wn.sub_workflow_ref._project = settings.project
        wn.sub_workflow_ref._domain = settings.domain

    def _mutate_node(n: workflow_model.Node):
        if n.task_node:
            _mutate_task_node(n.task_node)
        elif n.branch_node:
            _mutate_branch_node_task_ids(n.branch_node)
        elif n.workflow_node:
            _mutate_workflow_node(n.workflow_node)

    for n in entity.flyte_nodes:
        _mutate_node(n)

    entity.id._project = settings.project
    entity.id._domain = settings.domain

    return entity


def get_serializable_flyte_task(entity: "FlyteTask", settings: SerializationSettings) -> FlyteControlPlaneEntity:
    """
    TODO replace with deep copy
    """
    entity.id._project = settings.project
    entity.id._domain = settings.domain
    return entity


def get_serializable(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: FlyteLocalEntity,
    options: Optional[Options] = None,
) -> FlyteControlPlaneEntity:
    """
    The flytekit authoring code produces objects representing Flyte entities (tasks, workflows, etc.). In order to
    register these, they need to be converted into objects that Flyte Admin understands (the IDL objects basically, but
    this function currently translates to the layer above (e.g. SdkTask) - this will be changed to the IDL objects
    directly in the future).

    :param entity_mapping: This is an ordered dict that will be mutated in place. The reason this argument exists is
      because there is a natural ordering to the entities at registration time. That is, underlying tasks have to be
      registered before the workflows that use them. The recursive search done by this function and the functions
      above form a natural topological sort, finding the dependent entities and adding them to this parameter before
      the parent entity this function is called with.
    :param settings: used to pick up project/domain/name - to be deprecated.
    :param entity: The local flyte entity to try to convert (along with its dependencies)
    :param options: Optionally pass in a set of options that can be used to add additional metadata for Launchplans
    :return: The resulting control plane entity, in addition to being added to the mutable entity_mapping parameter
      is also returned.
    """
    if entity in entity_mapping:
        return entity_mapping[entity]

    from flytekit.remote import FlyteLaunchPlan, FlyteTask, FlyteWorkflow

    if isinstance(entity, ReferenceEntity):
        cp_entity = get_reference_spec(entity_mapping, settings, entity)

    elif isinstance(entity, PythonTask):
        cp_entity = get_serializable_task(entity_mapping, settings, entity)

    elif isinstance(entity, WorkflowBase):
        cp_entity = get_serializable_workflow(entity_mapping, settings, entity, options)

    elif isinstance(entity, Node):
        cp_entity = get_serializable_node(entity_mapping, settings, entity, options)

    elif isinstance(entity, LaunchPlan):
        cp_entity = get_serializable_launch_plan(entity_mapping, settings, entity, options=options)

    elif isinstance(entity, BranchNode):
        cp_entity = get_serializable_branch_node(entity_mapping, settings, entity, options)

    elif isinstance(entity, FlyteTask) or isinstance(entity, FlyteWorkflow):
        if entity.should_register:
            if isinstance(entity, FlyteTask):
                cp_entity = get_serializable_flyte_task(entity, settings)
            else:
                if entity.should_register:
                    # We only add the tasks if the should register flag is set. This is to avoid adding
                    # unnecessary tasks to the registrable list.
                    for t in entity.flyte_tasks:
                        get_serializable(entity_mapping, settings, t, options)
                cp_entity = get_serializable_flyte_workflow(entity, settings)
        else:
            cp_entity = entity

    elif isinstance(entity, FlyteLaunchPlan):
        cp_entity = entity

    elif isinstance(entity, ArrayNode):
        cp_entity = get_serializable_array_node(entity_mapping, settings, entity, options)

    else:
        raise ValueError(f"Non serializable type found {type(entity)} Entity {entity}")

    if isinstance(entity, TaskSpec) or isinstance(entity, WorkflowSpec):
        # 1. Check if the size of long description exceeds 16KB
        # 2. Extract the repo URL from the git config, and assign it to the link of the source code of the description entity
        if entity.docs and entity.docs.long_description:
            if entity.docs.long_description.value:
                if sys.getsizeof(entity.docs.long_description.value) > 16 * 1024 * 1024:
                    raise ValueError(
                        "Long Description of the flyte entity exceeds the 16KB size limit. Please specify the uri in the long description instead."
                    )
            entity.docs.source_code = SourceCode(link=settings.git_repo)
    # This needs to be at the bottom not the top - i.e. dependent tasks get added before the workflow containing it
    entity_mapping[entity] = cp_entity
    return cp_entity


def gather_dependent_entities(
    serialized: OrderedDict,
) -> Tuple[
    Dict[_identifier_model.Identifier, TaskTemplate],
    Dict[_identifier_model.Identifier, admin_workflow_models.WorkflowSpec],
    Dict[_identifier_model.Identifier, _launch_plan_models.LaunchPlanSpec],
]:
    """
    The ``get_serializable`` function above takes in an ``OrderedDict`` that helps keep track of dependent entities.
    For example, when serializing a workflow, all its tasks are also serialized. The ordered dict will also contain
    serialized entities that aren't as useful though, like nodes and branches. This is just a small helper function
    that will pull out the serialized tasks, workflows, and launch plans. This function is primarily used for testing.

    :param serialized: This should be the filled in OrderedDict used in the get_serializable function above.
    :return:
    """
    task_templates: Dict[_identifier_model.Identifier, TaskTemplate] = {}
    workflow_specs: Dict[_identifier_model.Identifier, admin_workflow_models.WorkflowSpec] = {}
    launch_plan_specs: Dict[_identifier_model.Identifier, _launch_plan_models.LaunchPlanSpec] = {}

    for cp_entity in serialized.values():
        if isinstance(cp_entity, TaskSpec):
            task_templates[cp_entity.template.id] = cp_entity.template
        elif isinstance(cp_entity, _launch_plan_models.LaunchPlan):
            launch_plan_specs[cp_entity.id] = cp_entity.spec
        elif isinstance(cp_entity, admin_workflow_models.WorkflowSpec):
            workflow_specs[cp_entity.template.id] = cp_entity

    return task_templates, workflow_specs, launch_plan_specs

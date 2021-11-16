from collections import OrderedDict
from typing import Callable, Dict, List, Optional, Tuple, Union

from flytekit.common import constants as _common_constants
from flytekit.common.utils import _dnsify
from flytekit.core.base_task import PythonTask
from flytekit.core.condition import BranchNode
from flytekit.core.context_manager import SerializationSettings
from flytekit.core.launch_plan import LaunchPlan, ReferenceLaunchPlan
from flytekit.core.node import Node
from flytekit.core.python_auto_container import PythonAutoContainerTask
from flytekit.core.reference_entity import ReferenceEntity, ReferenceSpec, ReferenceTemplate
from flytekit.core.task import ReferenceTask
from flytekit.core.workflow import ReferenceWorkflow, WorkflowBase
from flytekit.models import common as _common_models
from flytekit.models import interface as interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core import workflow as _core_wf
from flytekit.models.core import workflow as workflow_model
from flytekit.models.core.workflow import BranchNode as BranchNodeModel
from flytekit.models.core.workflow import TaskNodeOverrides

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
]
FlyteControlPlaneEntity = Union[
    task_models.TaskSpec,
    _launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
    workflow_model.Node,
    BranchNodeModel,
]


def to_serializable_case(
    entity_mapping: OrderedDict, settings: SerializationSettings, c: _core_wf.IfBlock
) -> _core_wf.IfBlock:
    if c is None:
        raise ValueError("Cannot convert none cases to registrable")
    then_node = get_serializable(entity_mapping, settings, c.then_node)
    return _core_wf.IfBlock(condition=c.condition, then_node=then_node)


def to_serializable_cases(
    entity_mapping: OrderedDict, settings: SerializationSettings, cases: List[_core_wf.IfBlock]
) -> Optional[List[_core_wf.IfBlock]]:
    if cases is None:
        return None
    ret_cases = []
    for c in cases:
        ret_cases.append(to_serializable_case(entity_mapping, settings, c))
    return ret_cases


def _fast_serialize_command_fn(
    settings: SerializationSettings, task: PythonAutoContainerTask
) -> Callable[[SerializationSettings], List[str]]:
    default_command = task.get_default_command(settings)

    dest_dir = (
        settings.fast_serialization_settings.destination_dir if settings.fast_serialization_settings is not None else ""
    )
    if dest_dir is None or dest_dir == "":
        dest_dir = "{{ .dest_dir }}"

    def fn(settings: SerializationSettings) -> List[str]:
        return [
            "pyflyte-fast-execute",
            "--additional-distribution",
            "{{ .remote_package_path }}",
            "--dest-dir",
            dest_dir,
            "--",
            *default_command,
        ]

    return fn


def get_serializable_task(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: FlyteLocalEntity,
) -> task_models.TaskSpec:
    task_id = _identifier_model.Identifier(
        _identifier_model.ResourceType.TASK,
        settings.project,
        settings.domain,
        entity.name,
        settings.version,
    )
    if settings.should_fast_serialize() and isinstance(entity, PythonAutoContainerTask):
        # For fast registration, we'll need to muck with the command, but only for certain kinds of tasks. Specifically,
        # tasks that rely on user code defined in the container. This should be encapsulated by the auto container
        # parent class
        entity.set_command_fn(_fast_serialize_command_fn(settings, entity))
    tt = task_models.TaskTemplate(
        id=task_id,
        type=entity.task_type,
        metadata=entity.metadata.to_taskmetadata_model(),
        interface=entity.interface,
        custom=entity.get_custom(settings),
        container=entity.get_container(settings),
        task_type_version=entity.task_type_version,
        security_context=entity.security_context,
        config=entity.get_config(settings),
        k8s_pod=entity.get_k8s_pod(settings),
        sql=entity.get_sql(settings),
    )
    if settings.should_fast_serialize() and isinstance(entity, PythonAutoContainerTask):
        entity.reset_command_fn()
    return task_models.TaskSpec(template=tt)


def get_serializable_workflow(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: WorkflowBase,
) -> admin_workflow_models.WorkflowSpec:
    # Get node models
    upstream_node_models = [
        get_serializable(entity_mapping, settings, n)
        for n in entity.nodes
        if n.id != _common_constants.GLOBAL_INPUT_NODE_ID
    ]

    sub_wfs = []
    for n in entity.nodes:
        if isinstance(n.flyte_entity, WorkflowBase):
            # We are currently not supporting reference workflows since these will
            # require a network call to flyteadmin to populate the WorkflowTemplate
            # object
            if isinstance(n.flyte_entity, ReferenceWorkflow):
                raise Exception(
                    "Reference sub-workflows are currently unsupported. Use reference launch plans instead."
                )
            sub_wf_spec = get_serializable(entity_mapping, settings, n.flyte_entity)
            if not isinstance(sub_wf_spec, admin_workflow_models.WorkflowSpec):
                raise TypeError(
                    f"Unexpected type for serialized form of workflow. Expected {admin_workflow_models.WorkflowSpec}, but got {type(sub_wf_spec)}"
                )
            sub_wfs.append(sub_wf_spec.template)
            sub_wfs.extend(sub_wf_spec.sub_workflows)

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
                    sub_wf_spec = get_serializable(entity_mapping, settings, leaf_node.flyte_entity)
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
        nodes=upstream_node_models,
        outputs=entity.output_bindings,
    )

    return admin_workflow_models.WorkflowSpec(template=wf_t, sub_workflows=list(set(sub_wfs)))


def get_serializable_launch_plan(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: LaunchPlan,
) -> _launch_plan_models.LaunchPlan:
    wf_spec = get_serializable(entity_mapping, settings, entity.workflow)

    lps = _launch_plan_models.LaunchPlanSpec(
        workflow_id=wf_spec.template.id,
        entity_metadata=_launch_plan_models.LaunchPlanMetadata(
            schedule=entity.schedule,
            notifications=entity.notifications,
        ),
        default_inputs=entity.parameters,
        fixed_inputs=entity.fixed_inputs,
        labels=entity.labels or _common_models.Labels({}),
        annotations=entity.annotations or _common_models.Annotations({}),
        auth_role=entity._auth_role or _common_models.AuthRole(),
        raw_output_data_config=entity.raw_output_data_config or _common_models.RawOutputDataConfig(""),
        max_parallelism=entity.max_parallelism,
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
    )

    return lp_model


def get_serializable_node(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: Node,
) -> workflow_model.Node:
    if entity.flyte_entity is None:
        raise Exception(f"Node {entity.id} has no flyte entity")

    upstream_sdk_nodes = [
        get_serializable(entity_mapping, settings, n)
        for n in entity.upstream_nodes
        if n.id != _common_constants.GLOBAL_INPUT_NODE_ID
    ]

    # Reference entities also inherit from the classes in the second if statement so address them first.
    if isinstance(entity.flyte_entity, ReferenceEntity):
        ref_spec = get_serializable(entity_mapping, settings, entity.flyte_entity)
        ref_template = ref_spec.template
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_sdk_nodes],
            output_aliases=[],
        )
        if ref_template.resource_type == _identifier_model.ResourceType.TASK:
            node_model._task_node = workflow_model.TaskNode(reference_id=ref_template.id)
        elif ref_template.resource_type == _identifier_model.ResourceType.WORKFLOW:
            node_model._workflow_node = workflow_model.WorkflowNode(sub_workflow_ref=ref_template.id)
        elif ref_template.resource_type == _identifier_model.ResourceType.LAUNCH_PLAN:
            node_model._workflow_node = workflow_model.WorkflowNode(launchplan_ref=ref_template.id)
        else:
            raise Exception(
                f"Unexpected resource type for reference entity {entity.flyte_entity}: {ref_template.resource_type}"
            )
        return node_model

    if isinstance(entity.flyte_entity, PythonTask):
        task_spec = get_serializable(entity_mapping, settings, entity.flyte_entity)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_sdk_nodes],
            output_aliases=[],
            task_node=workflow_model.TaskNode(
                reference_id=task_spec.template.id, overrides=TaskNodeOverrides(resources=entity._resources)
            ),
        )
        if entity._aliases:
            node_model._output_aliases = entity._aliases

    elif isinstance(entity.flyte_entity, WorkflowBase):
        wf_spec = get_serializable(entity_mapping, settings, entity.flyte_entity)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_sdk_nodes],
            output_aliases=[],
            workflow_node=workflow_model.WorkflowNode(sub_workflow_ref=wf_spec.template.id),
        )

    elif isinstance(entity.flyte_entity, BranchNode):
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_sdk_nodes],
            output_aliases=[],
            branch_node=get_serializable(entity_mapping, settings, entity.flyte_entity),
        )

    elif isinstance(entity.flyte_entity, LaunchPlan):
        lp_spec = get_serializable(entity_mapping, settings, entity.flyte_entity)

        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_sdk_nodes],
            output_aliases=[],
            workflow_node=workflow_model.WorkflowNode(launchplan_ref=lp_spec.id),
        )
    else:
        raise Exception(f"Node contained non-serializable entity {entity._flyte_entity}")

    return node_model


def get_serializable_branch_node(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: FlyteLocalEntity,
) -> BranchNodeModel:
    # We have to iterate through the blocks to convert the nodes from the internal Node type to the Node model type.
    # This was done to avoid having to create our own IfElseBlock object (i.e. condition.py just uses the model
    # directly) even though the node there is of the wrong type (our type instead of the model type).
    # TODO this should be cleaned up instead of mutation, we probably should just create a new object
    first = to_serializable_case(entity_mapping, settings, entity._ifelse_block.case)
    other = to_serializable_cases(entity_mapping, settings, entity._ifelse_block.other)
    else_node_model = None
    if entity._ifelse_block.else_node:
        else_node_model = get_serializable(entity_mapping, settings, entity._ifelse_block.else_node)

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


def get_serializable(
    entity_mapping: OrderedDict,
    settings: SerializationSettings,
    entity: FlyteLocalEntity,
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
    :return: The resulting control plane entity, in addition to being added to the mutable entity_mapping parameter
      is also returned.
    """
    if entity in entity_mapping:
        return entity_mapping[entity]

    if isinstance(entity, ReferenceEntity):
        cp_entity = get_reference_spec(entity_mapping, settings, entity)

    elif isinstance(entity, PythonTask):
        cp_entity = get_serializable_task(entity_mapping, settings, entity)

    elif isinstance(entity, WorkflowBase):
        cp_entity = get_serializable_workflow(entity_mapping, settings, entity)

    elif isinstance(entity, Node):
        cp_entity = get_serializable_node(entity_mapping, settings, entity)

    elif isinstance(entity, LaunchPlan):
        cp_entity = get_serializable_launch_plan(entity_mapping, settings, entity)

    elif isinstance(entity, BranchNode):
        cp_entity = get_serializable_branch_node(entity_mapping, settings, entity)
    else:
        raise Exception(f"Non serializable type found {type(entity)} Entity {entity}")

    # This needs to be at the bottom not the top - i.e. dependent tasks get added before the workflow containing it
    entity_mapping[entity] = cp_entity
    return cp_entity


def gather_dependent_entities(
    serialized: OrderedDict,
) -> Tuple[
    Dict[_identifier_model.Identifier, task_models.TaskTemplate],
    Dict[_identifier_model.Identifier, admin_workflow_models.WorkflowSpec],
    Dict[_identifier_model.Identifier, _launch_plan_models.LaunchPlanSpec],
]:
    """
    The ``get_serializable`` function above takes in an ``OrderedDict`` that helps keep track of dependent entities.
    For example, when serializing a workflow, all its tasks are also serialized. The ordered dict will also contain
    serialized entities that aren't as useful though, like nodes and branches. This is just a small helper function
    that will pull out the serialzed tasks, workflows, and launch plans. This function is primarily used for testing.

    :param serialized: This should be the filled in OrderedDict used in the get_serializable function above.
    :return:
    """
    task_templates: Dict[_identifier_model.Identifier, task_models.TaskTemplate] = {}
    workflow_specs: Dict[_identifier_model.Identifier, admin_workflow_models.WorkflowSpec] = {}
    launch_plan_specs: Dict[_identifier_model.Identifier, _launch_plan_models.LaunchPlanSpec] = {}

    for cp_entity in serialized.values():
        if isinstance(cp_entity, task_models.TaskSpec):
            task_templates[cp_entity.template.id] = cp_entity.template
        elif isinstance(cp_entity, _launch_plan_models.LaunchPlan):
            launch_plan_specs[cp_entity.id] = cp_entity.spec
        elif isinstance(cp_entity, admin_workflow_models.WorkflowSpec):
            workflow_specs[cp_entity.template.id] = cp_entity

    return task_templates, workflow_specs, launch_plan_specs

from typing import Dict, List, Optional, Union

from flytekit.annotated.base_task import PythonTask, TaskMetadata
from flytekit.annotated.condition import BranchNode
from flytekit.annotated.context_manager import SerializationSettings
from flytekit.annotated.launch_plan import LaunchPlan, ReferenceLaunchPlan
from flytekit.annotated.node import Node
from flytekit.annotated.python_function_task import PythonAutoContainerTask
from flytekit.annotated.reference_entity import ReferenceEntity
from flytekit.annotated.task import ReferenceTask
from flytekit.annotated.workflow import ReferenceWorkflow, Workflow, WorkflowFailurePolicy, WorkflowMetadata
from flytekit.common import constants as _common_constants
from flytekit.common.interface import TypedInterface
from flytekit.common.launch_plan import SdkLaunchPlan
from flytekit.common.nodes import SdkNode
from flytekit.common.tasks.task import SdkTask
from flytekit.common.workflow import SdkWorkflow
from flytekit.models import common as _common_models
from flytekit.models import interface as interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import literals as literal_models
from flytekit.models.common import RawOutputDataConfig
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core import workflow as _core_wf
from flytekit.models.core import workflow as workflow_model
from flytekit.models.core.workflow import BranchNode as BranchNodeModel

FlyteLocalEntity = Union[
    PythonTask,
    BranchNode,
    Node,
    LaunchPlan,
    Workflow,
    ReferenceWorkflow,
    ReferenceTask,
    ReferenceLaunchPlan,
    ReferenceEntity,
]
FlyteControlPlaneEntity = Union[SdkTask, SdkLaunchPlan, SdkWorkflow, SdkNode, BranchNodeModel]


def to_serializable_case(settings: SerializationSettings, c: _core_wf.IfBlock) -> _core_wf.IfBlock:
    if c is None:
        raise ValueError("Cannot convert none cases to registrable")
    then_node = get_serializable(settings, c.then_node)
    return _core_wf.IfBlock(condition=c.condition, then_node=then_node)


def to_serializable_cases(
    settings: SerializationSettings, cases: List[_core_wf.IfBlock]
) -> Optional[List[_core_wf.IfBlock]]:
    if cases is None:
        return None
    ret_cases = []
    for c in cases:
        ret_cases.append(to_serializable_case(settings, c))
    return ret_cases


# Can make this part of the function in the future.
GLOBAL_CACHE: Dict[FlyteLocalEntity, FlyteControlPlaneEntity] = {}


def get_serializable_references(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: bool
) -> FlyteControlPlaneEntity:
    # TODO: This entire function isn't necessary. We should just return None or raise an Exception or something.
    #   Reference entities should already exist on the Admin control plane - they should not be serialized/registered
    #   again. Indeed we don't actually have enough information to serialize it properly.

    if isinstance(entity, ReferenceTask):
        cp_entity = SdkTask(
            type="ignore",
            metadata=TaskMetadata().to_taskmetadata_model(),
            interface=entity.typed_interface,
            custom={},
            container=None,
        )

    elif isinstance(entity, ReferenceWorkflow):
        workflow_metadata = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)

        cp_entity = SdkWorkflow(
            nodes=[],  # Fake an empty list for nodes,
            id=entity.reference.id,
            metadata=workflow_metadata,
            metadata_defaults=workflow_model.WorkflowMetadataDefaults(),
            interface=entity.typed_interface,
            output_bindings=[],
        )

    elif isinstance(entity, ReferenceLaunchPlan):
        cp_entity = SdkLaunchPlan(
            workflow_id=None,
            entity_metadata=_launch_plan_models.LaunchPlanMetadata(schedule=None, notifications=[]),
            default_inputs=interface_models.ParameterMap({}),
            fixed_inputs=literal_models.LiteralMap({}),
            labels=_common_models.Labels({}),
            annotations=_common_models.Annotations({}),
            auth_role=_common_models.AuthRole(assumable_iam_role="fake:role"),
            raw_output_data_config=RawOutputDataConfig(""),
        )
        # Because of how SdkNodes work, it needs one of these interfaces
        # Hopefully this is more trickery that can be cleaned up in the future
        cp_entity._interface = TypedInterface.promote_from_model(entity.typed_interface)

    else:
        raise Exception("Invalid reference type when serializing")

    # Make sure we don't serialize this
    cp_entity._has_registered = True
    cp_entity.assign_name(entity.id.name)
    cp_entity._id = entity.id
    return cp_entity


def get_serializable_task(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: bool
) -> FlyteControlPlaneEntity:
    cp_entity = SdkTask(
        type=entity.task_type,
        metadata=entity.metadata.to_taskmetadata_model(),
        interface=entity.interface,
        custom=entity.get_custom(settings),
        container=entity.get_container(settings),
    )
    # Reset just to make sure it's what we give it
    cp_entity.id._project = settings.project
    cp_entity.id._domain = settings.domain
    cp_entity.id._name = entity.name
    cp_entity.id._version = settings.version

    # For fast registration, we'll need to muck with the command, but only for certain kinds of tasks. Specifically,
    # tasks that rely on user code defined in the container. This should be encapsulated by the auto container
    # parent class
    if fast and isinstance(entity, PythonAutoContainerTask):
        args = [
            "pyflyte-fast-execute",
            "--additional-distribution",
            "{{ .remote_package_path }}",
            "--dest-dir",
            "{{ .dest_dir }}",
            "--",
        ] + cp_entity.container.args[:]

        del cp_entity._container.args[:]
        cp_entity._container.args.extend(args)

    return cp_entity


def get_serializable_workflow(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: bool
) -> FlyteControlPlaneEntity:
    workflow_id = _identifier_model.Identifier(
        _identifier_model.ResourceType.WORKFLOW, settings.project, settings.domain, entity.name, settings.version
    )

    # Translate nodes
    upstream_sdk_nodes = [
        get_serializable(settings, n) for n in entity._nodes if n.id != _common_constants.GLOBAL_INPUT_NODE_ID
    ]

    cp_entity = SdkWorkflow(
        nodes=upstream_sdk_nodes,
        id=workflow_id,
        metadata=entity.workflow_metadata.to_flyte_model(),
        metadata_defaults=entity.workflow_metadata_defaults.to_flyte_model(),
        interface=entity._interface,
        output_bindings=entity._output_bindings,
    )
    # Reset just to make sure it's what we give it
    cp_entity.id._project = settings.project
    cp_entity.id._domain = settings.domain
    cp_entity.id._name = entity.name
    cp_entity.id._version = settings.version
    return cp_entity


def get_serializable_launch_plan(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: bool
) -> FlyteControlPlaneEntity:
    sdk_workflow = get_serializable(settings, entity.workflow)
    cp_entity = SdkLaunchPlan(
        workflow_id=sdk_workflow.id,
        entity_metadata=_launch_plan_models.LaunchPlanMetadata(
            schedule=entity.schedule, notifications=entity.notifications,
        ),
        default_inputs=entity.parameters,
        fixed_inputs=entity.fixed_inputs,
        labels=entity.labels or _common_models.Labels({}),
        annotations=entity.annotations or _common_models.Annotations({}),
        auth_role=entity._auth_role,
        raw_output_data_config=entity.raw_output_data_config,
    )

    # These two things are normally set to None in the SdkLaunchPlan constructor and filled in by
    # SdkRunnableLaunchPlan/the registration process, so we need to set them manually. The reason is because these
    # fields are not part of the underlying LaunchPlanSpec
    cp_entity._interface = sdk_workflow.interface
    cp_entity._id = _identifier_model.Identifier(
        resource_type=_identifier_model.ResourceType.LAUNCH_PLAN,
        project=settings.project,
        domain=settings.domain,
        name=entity.name,
        version=settings.version,
    )
    return cp_entity


def get_serializable_node(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: bool
) -> FlyteControlPlaneEntity:
    if entity._flyte_entity is None:
        raise Exception(f"Node {entity.id} has no flyte entity")

    upstream_sdk_nodes = [
        get_serializable(settings, n) for n in entity._upstream_nodes if n.id != _common_constants.GLOBAL_INPUT_NODE_ID
    ]

    if isinstance(entity._flyte_entity, PythonTask):
        cp_entity = SdkNode(
            entity._id,
            upstream_nodes=upstream_sdk_nodes,
            bindings=entity._bindings,
            metadata=entity._metadata,
            sdk_task=get_serializable(settings, entity._flyte_entity, fast),
        )
        if entity._aliases:
            cp_entity._output_aliases = entity._aliases
    elif isinstance(entity._flyte_entity, Workflow):
        cp_entity = SdkNode(
            entity._id,
            upstream_nodes=upstream_sdk_nodes,
            bindings=entity._bindings,
            metadata=entity._metadata,
            sdk_workflow=get_serializable(settings, entity._flyte_entity),
        )
    elif isinstance(entity._flyte_entity, BranchNode):
        cp_entity = SdkNode(
            entity._id,
            upstream_nodes=upstream_sdk_nodes,
            bindings=entity._bindings,
            metadata=entity._metadata,
            sdk_branch=get_serializable(settings, entity._flyte_entity),
        )
    elif isinstance(entity._flyte_entity, LaunchPlan):
        cp_entity = SdkNode(
            entity._id,
            upstream_nodes=upstream_sdk_nodes,
            bindings=entity._bindings,
            metadata=entity._metadata,
            sdk_launch_plan=get_serializable(settings, entity._flyte_entity),
        )
    else:
        raise Exception(f"Node contained non-serializable entity {entity._flyte_entity}")

    return cp_entity


def get_serializable_branch_node(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: bool
) -> FlyteControlPlaneEntity:
    # We have to iterate through the blocks to convert the nodes from their current type to SDKNode
    # TODO this should be cleaned up instead of mutation, we probaby should just create a new object
    first = to_serializable_case(settings, entity._ifelse_block.case)
    other = to_serializable_cases(settings, entity._ifelse_block.other)
    else_node = None
    if entity._ifelse_block.else_node:
        else_node = get_serializable(settings, entity._ifelse_block.else_node)

    return BranchNodeModel(
        if_else=_core_wf.IfElseBlock(case=first, other=other, else_node=else_node, error=entity._ifelse_block.error)
    )


def get_serializable(
    settings: SerializationSettings, entity: FlyteLocalEntity, fast: Optional[bool] = False
) -> FlyteControlPlaneEntity:
    if entity in GLOBAL_CACHE:
        return GLOBAL_CACHE[entity]

    if isinstance(entity, ReferenceEntity):
        cp_entity = get_serializable_references(settings, entity, fast)

    elif isinstance(entity, PythonTask):
        cp_entity = get_serializable_task(settings, entity, fast)

    elif isinstance(entity, Workflow):
        cp_entity = get_serializable_workflow(settings, entity, fast)

    elif isinstance(entity, Node):
        cp_entity = get_serializable_node(settings, entity, fast)

    elif isinstance(entity, LaunchPlan):
        cp_entity = get_serializable_launch_plan(settings, entity, fast)

    elif isinstance(entity, BranchNode):
        cp_entity = get_serializable_branch_node(settings, entity, fast)
    else:
        raise Exception(f"Non serializable type found {type(entity)} Entity {entity}")

    GLOBAL_CACHE[entity] = cp_entity
    return cp_entity

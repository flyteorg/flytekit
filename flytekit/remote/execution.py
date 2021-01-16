from dataclasses import dataclass
from typing import Any, Dict, List, Union

from flytekit import ContainerTask
from flytekit.annotated import context_manager as flyte_context
from flytekit.annotated.context_manager import SerializationSettings
from flytekit.annotated.promise import translate_inputs_to_literals
from flytekit.annotated.reference_entity import ReferenceEntity, TaskReference
from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.common.core.identifier import WorkflowExecutionIdentifier, Identifier
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.translator import get_serializable
from flytekit.common.types.helpers import pack_python_std_map_to_literal_map, get_sdk_type_from_literal_type
from flytekit.common.workflow_execution import SdkWorkflowExecution
from flytekit.configuration import auth as auth_config
from flytekit.configuration import platform as platform_config
from flytekit.models.common import Annotations, AuthRole, Labels, Notification
from flytekit.models.core.identifier import ResourceType
from flytekit.models.execution import ExecutionMetadata, ExecutionSpec, NotificationList
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskSpec


@dataclass
class ExecutionParameters(object):
    notifications: List[Notification] = None
    labels: Labels = None
    annotations: Annotations = None
    auth_role: AuthRole = None
    # optional name to assign to execution
    name: str = None
    # The name of the user launching this execution.
    principal: str = None


def execute(
    entity: Union[Identifier, ContainerTask],
    project: str,
    domain: str,
    inputs: Dict[str, Any] = None,
    client: SynchronousFlyteClient = None,
    exec_parameters: ExecutionParameters = None,
    serialization_settings: SerializationSettings = None,
) -> SdkWorkflowExecution:

    if client is None:
        client = SynchronousFlyteClient(platform_config.URL.get, insecure=platform_config.INSECURE.get())

    # If serialization settings are supplied, first serialize and register the entity before attempting to launch
    # an execution with it.
    # If the entity already exists as is, that's fine. We will proceed to execute it regardless.
    if isinstance(entity, ContainerTask) and serialization_settings is not None:
        with flyte_context.FlyteContext.current_context().new_serialization_settings(
            serialization_settings=serialization_settings
        ) as ctx:
            sdk_task = get_serializable(ctx.serialization_settings, entity)
            reference_identifier = sdk_task.id
            try:
                client.create_task(task_identifer=sdk_task.id, task_spec=TaskSpec(sdk_task))
                literals = translate_inputs_to_literals(
                    ctx, input_kwargs=inputs, interface=entity.interface, native_input_types=entity.get_input_types()
                )

                input_literal_map = LiteralMap(literals=literals)
            except user_exceptions.FlyteEntityAlreadyExistsException:
                pass
    else:
        reference_identifier = entity
        if entity.resource_type == ResourceType.TASK:
            task_model = client.get_task(entity)
            input_literal_map = pack_python_std_map_to_literal_map(
                inputs,
                {
                    k: get_sdk_type_from_literal_type(v.type)
                    for k, v in task_model.closure.compiled_task.template.interface.inputs.items()
                },
            )
        elif entity.resource_type == ResourceType.LAUNCH_PLAN:
            lp_model = client.get_launch_plan(entity)
            # TODO: fetch reference workflow and use that interface.

    exec_parameters = exec_parameters if exec_parameters is not None else ExecutionParameters()
    disable_all = exec_parameters.notifications == []
    if disable_all:
        notifications = None
    else:
        notifications = NotificationList(exec_parameters.notifications or [])
        disable_all = None

    if exec_parameters.auth_role is not None:
        auth_role = exec_parameters.auth_role
    else:
        assumable_iam_role = auth_config.ASSUMABLE_IAM_ROLE.get()
        kubernetes_service_account = auth_config.KUBERNETES_SERVICE_ACCOUNT.get()
        auth_role = AuthRole(
            assumable_iam_role=assumable_iam_role, kubernetes_service_account=kubernetes_service_account
        )

    try:
        complete_exec_id = client.create_execution(
            project,
            domain,
            exec_parameters.name,
            ExecutionSpec(
                reference_identifier,
                # TODO: Detect nesting
                # Note: the principal can be derived from the authenticated user context.
                ExecutionMetadata(
                    ExecutionMetadata.ExecutionMode.MANUAL, principal=exec_parameters.principal, nesting=0
                ),
                notifications=notifications,
                disable_all=disable_all,
                labels=exec_parameters.labels,
                annotations=exec_parameters.annotations,
                auth_role=auth_role,
            ),
            input_literal_map,
        )
    except user_exceptions.FlyteEntityAlreadyExistsException:
        complete_exec_id = WorkflowExecutionIdentifier(project, domain, exec_parameters.name)
    execution = client.get_execution(complete_exec_id)
    return SdkWorkflowExecution.promote_from_model(execution)

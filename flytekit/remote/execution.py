from dataclasses import dataclass
from typing import Any, Dict, Union, List

from flytekit import ContainerTask
from flytekit.annotated.context_manager import SerializationSettings
from flytekit.annotated.promise import translate_inputs_to_literals
from flytekit.annotated.reference_entity import ReferenceEntity
from flytekit.common.core.identifier import WorkflowExecutionIdentifier
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.translator import get_serializable
from flytekit.common.workflow_execution import SdkWorkflowExecution
from flytekit.models.common import Notification, Annotations, Labels, AuthRole
from flytekit.models.execution import ExecutionSpec, ExecutionMetadata, NotificationList
from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.models.literals import LiteralMap
from flytekit.annotated import context_manager as flyte_context
from flytekit.models.task import TaskSpec
from flytekit.configuration import auth as auth_config


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
    entity: Union[ReferenceEntity, ContainerTask],
    client: SynchronousFlyteClient,
    project: str,
    domain: str,
    inputs: Dict[str, Any] = None,
    exec_parameters: ExecutionParameters = None,
    serialization_settings: SerializationSettings = None,
) -> SdkWorkflowExecution:

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
            except user_exceptions.FlyteEntityAlreadyExistsException:
                pass
    else:
        reference_identifier = entity.id

    with flyte_context.FlyteContext.current_context():
        literals = translate_inputs_to_literals(
            ctx, input_kwargs=inputs, interface=entity.interface, native_input_types=entity.get_input_types()
        )
        input_literal_map = LiteralMap(literals=literals)

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

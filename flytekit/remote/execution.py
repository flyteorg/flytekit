from dataclasses import dataclass
from typing import Any, Dict, Union, List

from flytekit import ContainerTask
from flytekit.common.core.identifier import Identifier
from flytekit.common.launch_plan import SdkLaunchPlan
from flytekit.common.tasks.task import SdkTask
from flytekit.common.workflow_execution import SdkWorkflowExecution
from flytekit.models.common import Notification, Annotations, Labels, AuthRole
from flytekit.models.execution import ExecutionSpec, ExecutionMetadata

from flytekit.engines.flyte import engine as _flyte_engine


@dataclass
class ExecutionParameters(object):
    identifier: Identifier
    notifications: List[Notification]
    labels: Labels
    annotations: Annotations
    auth_role: AuthRole
    # Optional name to assign to an execution


def _execute_entity(
    entity: Union[SdkTask, SdkLaunchPlan], inputs: Dict[str, Any] = None, parameters: ExecutionParameters = None
) -> SdkWorkflowExecution:

    sdk_workflow_execution = entity.launch(
        project=parameters.identifier.project,
        domain=parameters.identifier.domain,
        name=parameters.identifier.name,
        inputs=inputs,
        annotation_overrides=parameters.annotations,
        label_overrides=parameters.labels,
    )
    return sdk_workflow_execution


def execute_task(
    task: ContainerTask,
    project: str,
    domain: str,
    version: str,
    inputs: Dict[str, Any] = None,
    parameters: ExecutionParameters = None,
) -> SdkWorkflowExecution:
    """
    sdk_task = get_serializable(RegistrationSettings(project=project, ...), task)
    sdk_task = hydrate_task(sdk_task, project, domain, version)
    client = get_client(...) # initialize client from config
    # client.create_task(...) # swallow already exists
    # return _execute_entity(sdk_task, inputs)
    """

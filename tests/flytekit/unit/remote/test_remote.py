import pytest
from mock import MagicMock, patch

from flytekit.models.admin.workflow import Workflow
from flytekit.models.core.identifier import Identifier, ResourceType, WorkflowExecutionIdentifier
from flytekit.models.execution import Execution
from flytekit.models.launch_plan import LaunchPlan
from flytekit.models.task import Task
from flytekit.remote.remote import FlyteRemote

CLIENT_METHODS = {
    ResourceType.WORKFLOW: "list_workflows_paginated",
    ResourceType.TASK: "list_tasks_paginated",
    ResourceType.LAUNCH_PLAN: "list_launch_plans_paginated",
}

REMOTE_METHODS = {
    ResourceType.WORKFLOW: "fetch_workflow",
    ResourceType.TASK: "fetch_task",
    ResourceType.LAUNCH_PLAN: "fetch_launch_plan",
}

ENTITY_TYPE_TEXT = {
    ResourceType.WORKFLOW: "Workflow",
    ResourceType.TASK: "Task",
    ResourceType.LAUNCH_PLAN: "Launch Plan",
}


@patch("flytekit.engines.flyte.engine._FlyteClientManager")
@patch("flytekit.configuration.platform.URL")
@pytest.mark.parametrize(
    "entity_cls,resource_type",
    [
        [Workflow, ResourceType.WORKFLOW],
        [Task, ResourceType.TASK],
        [LaunchPlan, ResourceType.LAUNCH_PLAN],
    ],
)
def test_remote_fetch_execute_entities_task_workflow_launchplan(
    mock_url,
    mock_client_manager,
    entity_cls,
    resource_type,
):
    mock_url.get.return_value = "localhost"
    admin_entities = [
        entity_cls(
            Identifier(resource_type, "p1", "d1", "n1", version),
            *([MagicMock()] if resource_type != ResourceType.LAUNCH_PLAN else [MagicMock(), MagicMock()]),
        )
        for version in ["latest", "old"]
    ]

    mock_client = MagicMock()
    getattr(mock_client, CLIENT_METHODS[resource_type]).return_value = admin_entities, ""
    mock_client_manager.return_value.client = mock_client

    remote = FlyteRemote.from_environment()
    fetch_method = getattr(remote, REMOTE_METHODS[resource_type])
    flyte_entity_latest = fetch_method("p1", "d1", "n1", "latest")
    flyte_entity_latest_implicit = fetch_method("p1", "d1", "n1")
    flyte_entity_old = fetch_method("p1", "d1", "old")

    assert flyte_entity_latest.entity_type_text == ENTITY_TYPE_TEXT[resource_type]
    assert flyte_entity_latest.id == admin_entities[0].id
    assert flyte_entity_latest.id == flyte_entity_latest_implicit.id
    assert flyte_entity_latest.id != flyte_entity_old.id


@patch("flytekit.engines.flyte.engine._FlyteClientManager")
@patch("flytekit.configuration.platform.URL")
def test_remote_fetch_workflow_execution(mock_url, mock_client_manager):
    mock_url.get.return_value = "localhost"
    admin_workflow_execution = Execution(
        id=WorkflowExecutionIdentifier("p1", "d1", "n1"),
        spec=MagicMock(),
        closure=MagicMock(),
    )
    mock_client = MagicMock()
    mock_client.get_execution.return_value = admin_workflow_execution
    mock_client_manager.return_value.client = mock_client

    remote = FlyteRemote.from_environment()
    flyte_workflow_execution = remote.fetch_workflow_execution("p1", "d1", "n1")
    assert flyte_workflow_execution.id == admin_workflow_execution.id

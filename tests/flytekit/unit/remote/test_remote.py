import os

import pytest
from mock import MagicMock, patch

from flytekit.common.exceptions import user as user_exceptions
from flytekit.configuration import internal
from flytekit.models import common as common_models
from flytekit.models.admin.workflow import Workflow
from flytekit.models.core.identifier import (
    Identifier,
    NodeExecutionIdentifier,
    ResourceType,
    WorkflowExecutionIdentifier,
)
from flytekit.models.execution import Execution
from flytekit.models.interface import TypedInterface, Variable
from flytekit.models.launch_plan import LaunchPlan
from flytekit.models.node_execution import NodeExecution, NodeExecutionMetaData
from flytekit.models.task import Task
from flytekit.models.types import LiteralType, SimpleType
from flytekit.remote import FlyteWorkflow
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


@patch("flytekit.clients.friendly.SynchronousFlyteClient")
@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
@pytest.mark.parametrize(
    "entity_cls,resource_type",
    [
        [Workflow, ResourceType.WORKFLOW],
        [Task, ResourceType.TASK],
        [LaunchPlan, ResourceType.LAUNCH_PLAN],
    ],
)
def test_remote_fetch_execute_entities_task_workflow_launchplan(
    mock_insecure,
    mock_url,
    mock_client,
    entity_cls,
    resource_type,
):
    admin_entities = [
        entity_cls(
            Identifier(resource_type, "p1", "d1", "n1", version),
            *([MagicMock()] if resource_type != ResourceType.LAUNCH_PLAN else [MagicMock(), MagicMock()]),
        )
        for version in ["latest", "old"]
    ]

    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True
    mock_client = MagicMock()
    getattr(mock_client, CLIENT_METHODS[resource_type]).return_value = admin_entities, ""

    remote = FlyteRemote.from_config("p1", "d1")
    remote._client = mock_client
    fetch_method = getattr(remote, REMOTE_METHODS[resource_type])
    flyte_entity_latest = fetch_method(name="n1", version="latest")
    flyte_entity_latest_implicit = fetch_method(name="n1")
    flyte_entity_old = fetch_method(name="n1", version="old")

    assert flyte_entity_latest.entity_type_text == ENTITY_TYPE_TEXT[resource_type]
    assert flyte_entity_latest.id == admin_entities[0].id
    assert flyte_entity_latest.id == flyte_entity_latest_implicit.id
    assert flyte_entity_latest.id != flyte_entity_old.id


@patch("flytekit.clients.friendly.SynchronousFlyteClient")
@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_remote_fetch_workflow_execution(mock_insecure, mock_url, mock_client_manager):
    admin_workflow_execution = Execution(
        id=WorkflowExecutionIdentifier("p1", "d1", "n1"),
        spec=MagicMock(),
        closure=MagicMock(),
    )

    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True
    mock_client = MagicMock()
    mock_client.get_execution.return_value = admin_workflow_execution

    remote = FlyteRemote.from_config("p1", "d1")
    remote._client = mock_client
    flyte_workflow_execution = remote.fetch_workflow_execution(name="n1")
    assert flyte_workflow_execution.id == admin_workflow_execution.id


@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_get_node_execution_interface(mock_insecure, mock_url):
    expected_interface = TypedInterface(
        {"in1": Variable(LiteralType(simple=SimpleType.STRING), "in1 description")},
        {"out1": Variable(LiteralType(simple=SimpleType.INTEGER), "out1 description")},
    )

    node_exec_id = NodeExecutionIdentifier("node_id", WorkflowExecutionIdentifier("p1", "d1", "exec_name"))

    mock_node = MagicMock()
    mock_node.id = node_exec_id.node_id
    task_node = MagicMock()
    flyte_task = MagicMock()
    flyte_task.interface = expected_interface
    task_node.flyte_task = flyte_task
    mock_node.task_node = task_node

    flyte_workflow = FlyteWorkflow([mock_node], None, None, None, None, None)

    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote.from_config("p1", "d1")
    remote._client = mock_client
    actual_interface = remote._get_node_execution_interface(
        NodeExecution(node_exec_id, None, None, NodeExecutionMetaData(None, True, None)), flyte_workflow
    )
    assert actual_interface == expected_interface


@patch("flytekit.remote.workflow_execution.FlyteWorkflowExecution.promote_from_model")
@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_underscore_execute_uses_launch_plan_attributes(mock_insecure, mock_url, mock_wf_exec):
    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True
    mock_wf_exec.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote.from_config("p1", "d1")
    remote._client = mock_client

    def local_assertions(*args, **kwargs):
        execution_spec = args[3]
        assert execution_spec.auth_role.kubernetes_service_account == "svc"
        assert execution_spec.labels == common_models.Labels({"a": "my_label_value"})
        assert execution_spec.annotations == common_models.Annotations({"b": "my_annotation_value"})

    mock_client.create_execution.side_effect = local_assertions

    mock_entity = MagicMock()

    remote._execute(
        mock_entity,
        inputs={},
        project="proj",
        domain="dev",
        labels=common_models.Labels({"a": "my_label_value"}),
        annotations=common_models.Annotations({"b": "my_annotation_value"}),
        auth_role=common_models.AuthRole(kubernetes_service_account="svc"),
    )


@patch("flytekit.remote.workflow_execution.FlyteWorkflowExecution.promote_from_model")
@patch("flytekit.configuration.auth.ASSUMABLE_IAM_ROLE")
@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_underscore_execute_fall_back_remote_attributes(mock_insecure, mock_url, mock_iam_role, mock_wf_exec):
    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True
    mock_iam_role.get.return_value = "iam:some:role"
    mock_wf_exec.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote.from_config("p1", "d1")
    remote._client = mock_client

    def local_assertions(*args, **kwargs):
        execution_spec = args[3]
        assert execution_spec.auth_role.assumable_iam_role == "iam:some:role"

    mock_client.create_execution.side_effect = local_assertions

    mock_entity = MagicMock()

    remote._execute(
        mock_entity,
        inputs={},
        project="proj",
        domain="dev",
    )


@patch("flytekit.remote.workflow_execution.FlyteWorkflowExecution.promote_from_model")
@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_execute_with_wrong_input_key(mock_insecure, mock_url, mock_wf_exec):
    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True
    mock_wf_exec.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote.from_config("p1", "d1")
    remote._client = mock_client

    mock_entity = MagicMock()
    mock_entity.interface.inputs = {"foo": int}

    with pytest.raises(user_exceptions.FlyteValueException):
        remote._execute(
            mock_entity,
            inputs={"bar": 3},
            project="proj",
            domain="dev",
        )


@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_form_config(mock_insecure, mock_url):
    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = True

    FlyteRemote.from_config("p1", "d1")
    assert ".flyte/config" in os.environ[internal.CONFIGURATION_PATH.env_var]
    remote = FlyteRemote.from_config("p1", "d1", "fake_config")
    assert "fake_config" in os.environ[internal.CONFIGURATION_PATH.env_var]

    assert remote._flyte_admin_url == "localhost"
    assert remote._insecure is True
    assert remote.default_project == "p1"
    assert remote.default_domain == "d1"

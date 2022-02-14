import os

import pytest
from mock import MagicMock, patch

from flytekit.configuration import internal
from flytekit.exceptions import user as user_exceptions
from flytekit.models import common as common_models
from flytekit.models.core.identifier import ResourceType, WorkflowExecutionIdentifier
from flytekit.models.execution import Execution
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


@patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")
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


@patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")
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


@patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")
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


@patch("flytekit.clients.raw._ssl_channel_credentials")
@patch("flytekit.clients.raw._secure_channel")
@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_explicit_grpc_channel_credentials(mock_insecure, mock_url, mock_secure_channel, mock_ssl_channel_credentials):
    mock_url.get.return_value = "localhost"
    mock_insecure.get.return_value = False

    # Default mode, no explicit channel credentials
    mock_ssl_channel_credentials.reset_mock()
    _ = FlyteRemote.from_config("project", "domain")

    assert mock_ssl_channel_credentials.called

    mock_secure_channel.reset_mock()
    mock_ssl_channel_credentials.reset_mock()

    # Explicit channel credentials
    from grpc import ssl_channel_credentials

    credentials = ssl_channel_credentials(b"TEST CERTIFICATE")

    _ = FlyteRemote.from_config("project", "domain", grpc_credentials=credentials)
    assert mock_secure_channel.called
    assert mock_secure_channel.call_args[0][1] == credentials
    assert mock_ssl_channel_credentials.call_count == 1


# def test_vjkl():
#     rr = FlyteRemote.from_config("flytesnacks", "development", config_file_path="/Users/ytong/.flyte/local_sandbox")
#     wf = rr.fetch_workflow(name="core.control_flow.run_merge_sort.merge_sort", version="v0.3.39")
#
#
# def test_jasilv():
#     rr = FlyteRemote.from_config("flytesnacks", "development", config_file_path="/Users/ytong/.flyte/local_sandbox")
#     # wf = rr.fetch_workflow(
#     #     name="core.control_flow.run_conditions.nested_conditions", version="v0.3.39"
#     # )
#     # print(wf)
#     we = rr.fetch_workflow_execution(name="suss7xonol")
#     rr.sync_workflow_execution(we, sync_nodes=True)
#     print(rr)

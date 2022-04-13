import pytest
from mock import MagicMock, patch

from flytekit.configuration import Config
from flytekit.exceptions import user as user_exceptions
from flytekit.models import common as common_models
from flytekit.models.core.identifier import ResourceType, WorkflowExecutionIdentifier
from flytekit.models.execution import Execution
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.translator import Options

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
def test_remote_fetch_execution(mock_client_manager):
    admin_workflow_execution = Execution(
        id=WorkflowExecutionIdentifier("p1", "d1", "n1"),
        spec=MagicMock(),
        closure=MagicMock(),
    )

    mock_client = MagicMock()
    mock_client.get_execution.return_value = admin_workflow_execution

    remote = FlyteRemote(config=Config.auto(), default_project="p1", default_domain="d1")
    remote._client = mock_client
    flyte_workflow_execution = remote.fetch_execution(name="n1")
    assert flyte_workflow_execution.id == admin_workflow_execution.id


@patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")
def test_underscore_execute_uses_launch_plan_attributes(mock_wf_exec):
    mock_wf_exec.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote(config=Config.auto(), default_project="p1", default_domain="d1")
    remote._client = mock_client

    def local_assertions(*args, **kwargs):
        execution_spec = args[3]
        assert execution_spec.auth_role.kubernetes_service_account == "svc"
        assert execution_spec.labels == common_models.Labels({"a": "my_label_value"})
        assert execution_spec.annotations == common_models.Annotations({"b": "my_annotation_value"})

    mock_client.create_execution.side_effect = local_assertions

    mock_entity = MagicMock()
    options = Options(
        labels=common_models.Labels({"a": "my_label_value"}),
        annotations=common_models.Annotations({"b": "my_annotation_value"}),
        auth_role=common_models.AuthRole(kubernetes_service_account="svc"),
    )

    remote._execute(
        mock_entity,
        inputs={},
        project="proj",
        domain="dev",
        options=options,
    )


@patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")
def test_underscore_execute_fall_back_remote_attributes(mock_wf_exec):
    mock_wf_exec.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote(config=Config.auto(), default_project="p1", default_domain="d1")
    remote._client = mock_client

    options = Options(
        raw_output_data_config=common_models.RawOutputDataConfig(output_location_prefix="raw_output"),
        auth_role=common_models.AuthRole(assumable_iam_role="iam:some:role"),
    )

    def local_assertions(*args, **kwargs):
        execution_spec = args[3]
        assert execution_spec.auth_role.assumable_iam_role == "iam:some:role"
        assert execution_spec.raw_output_data_config.output_location_prefix == "raw_output"

    mock_client.create_execution.side_effect = local_assertions

    mock_entity = MagicMock()

    remote._execute(
        mock_entity,
        inputs={},
        project="proj",
        domain="dev",
        options=options,
    )


@patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")
def test_execute_with_wrong_input_key(mock_wf_exec):
    # mock_url.get.return_value = "localhost"
    # mock_insecure.get.return_value = True
    mock_wf_exec.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote(config=Config.auto(), default_project="p1", default_domain="d1")
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


def test_form_config():
    remote = FlyteRemote(config=Config.auto(), default_project="p1", default_domain="d1")
    assert remote.default_project == "p1"
    assert remote.default_domain == "d1"


@patch("flytekit.remote.remote.SynchronousFlyteClient")
def test_passing_of_kwargs(mock_client):
    additional_args = {
        "credentials": 1,
        "options": 2,
        "private_key": 3,
        "compression": 4,
        "root_certificates": 5,
        "certificate_chain": 6,
    }
    FlyteRemote(config=Config.auto(), default_project="project", default_domain="domain", **additional_args)
    assert mock_client.called
    assert mock_client.call_args[1] == additional_args

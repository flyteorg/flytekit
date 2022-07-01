import os
import pathlib
import tempfile

import numpy as np
import pandas as pd
import pytest
from mock import MagicMock, patch

import flytekit.configuration
from flytekit import kwtypes, task, workflow
from flytekit.configuration import Config, DefaultImages, ImageConfig
from flytekit.exceptions import user as user_exceptions
from flytekit.extras.sqlite3.task import SQLite3Config, SQLite3Task
from flytekit.models import common as common_models
from flytekit.models import security
from flytekit.models.core.identifier import ResourceType, WorkflowExecutionIdentifier
from flytekit.models.execution import Execution
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.translator import Options
from flytekit.types.schema import FlyteSchema

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


@pytest.fixture
def sql_task_fixture():
    EXAMPLE_DB = "https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"

    sql_task = SQLite3Task(
        name="cookbook.sqlite3.sample",
        query_template="select TrackId, Name from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLite3Config(uri=EXAMPLE_DB, compressed=True),
    )

    return sql_task


@pytest.fixture
def python_task_fixture():
    @task
    def generate_normal_df(n: int, mean: float, sigma: float) -> pd.DataFrame:
        return pd.DataFrame({"numbers": np.random.normal(mean, sigma, size=n)})

    return generate_normal_df


@pytest.fixture
def remote():
    remote = FlyteRemote(Config.auto(), default_project="flytesnacks", default_domain="staging")
    return remote


@patch("flytekit.clients.friendly.SynchronousFlyteClient")
def test_sql_task_registration_without_serialization_settings(mock_client, sql_task_fixture, remote):
    remote._client = mock_client

    rt = remote.register_task(entity=sql_task_fixture, version="v1")

    assert rt.id.project == "flytesnacks"
    assert rt.id.domain == "staging"
    assert rt.id.name == "cookbook.sqlite3.sample"
    assert rt.id.version == "v1"


def test_python_task_registration_without_serialization_settings(python_task_fixture, remote):
    with pytest.raises(ValueError) as excinfo:
        remote.register_task(entity=python_task_fixture, version="v1")

    assert str(excinfo.value) == "An image is required for PythonAutoContainer tasks"


@patch("flytekit.clients.friendly.SynchronousFlyteClient")
def test_workflow_registration_with_only_sql_task_and_without_serialization_settings(
    mock_client, sql_task_fixture, remote
):
    @workflow
    def only_sql_wf() -> pd.DataFrame:
        return sql_task_fixture(limit=100)

    remote._client = mock_client

    remote.register_workflow(entity=only_sql_wf, version="v1")


def test_workflow_registration_with_sql_and_python_task_and_without_serialization_settings(sql_task_fixture, remote):
    @task
    def print_and_count_columns(df: pd.DataFrame) -> int:
        return len(df[df.columns[0]])

    @workflow
    def sql_and_python_wf() -> int:
        return print_and_count_columns(df=sql_task_fixture(limit=100))

    with pytest.raises(ValueError) as excinfo:
        remote.register_workflow(entity=sql_and_python_wf, version="v1")

    assert str(excinfo.value) == "An image is required for PythonAutoContainer tasks"


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
        assert execution_spec.security_context.run_as.k8s_service_account == "svc"
        assert execution_spec.labels == common_models.Labels({"a": "my_label_value"})
        assert execution_spec.annotations == common_models.Annotations({"b": "my_annotation_value"})

    mock_client.create_execution.side_effect = local_assertions

    mock_entity = MagicMock()
    options = Options(
        labels=common_models.Labels({"a": "my_label_value"}),
        annotations=common_models.Annotations({"b": "my_annotation_value"}),
        security_context=security.SecurityContext(run_as=security.Identity(k8s_service_account="svc")),
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
        security_context=security.SecurityContext(run_as=security.Identity(iam_role="iam:some:role")),
    )

    def local_assertions(*args, **kwargs):
        execution_spec = args[3]
        assert execution_spec.security_context.run_as.iam_role == "iam:some:role"
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


@patch("flytekit.remote.remote.SynchronousFlyteClient")
def test_more_stuff(mock_client):
    r = FlyteRemote(config=Config.auto(), default_project="project", default_domain="domain")

    # Can't upload a folder
    with pytest.raises(ValueError):
        with tempfile.TemporaryDirectory() as tmp_dir:
            r._upload_file(pathlib.Path(tmp_dir))

    # Test that this copies the file.
    with tempfile.TemporaryDirectory() as tmp_dir:
        mm = MagicMock()
        mm.signed_url = os.path.join(tmp_dir, "tmp_file")
        mock_client.return_value.get_upload_signed_url.return_value = mm

        r._upload_file(pathlib.Path(__file__))

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig.auto(img_name=DefaultImages.default_image()),
    )

    # gives a thing
    computed_v = r._version_from_hash(b"", serialization_settings)
    assert len(computed_v) > 0

    # gives the same thing
    computed_v2 = r._version_from_hash(b"", serialization_settings)
    assert computed_v2 == computed_v2

    # should give a different thing
    computed_v3 = r._version_from_hash(b"", serialization_settings, "hi")
    assert computed_v2 != computed_v3


@patch("flytekit.remote.remote.SynchronousFlyteClient")
def test_generate_http_domain_sandbox_rewrite(mock_client):
    _, temp_filename = tempfile.mkstemp(suffix=".yaml")
    with open(temp_filename, "w") as f:
        # This string is similar to the relevant configuration emitted by flytectl in the cases of both demo and sandbox.
        flytectl_config_file = """admin:
    endpoint: localhost:30081
    authType: Pkce
    insecure: true
        """
        f.write(flytectl_config_file)

    remote = FlyteRemote(
        config=Config.auto(config_file=temp_filename), default_project="project", default_domain="domain"
    )
    assert remote.generate_http_domain() == "http://localhost:30080"

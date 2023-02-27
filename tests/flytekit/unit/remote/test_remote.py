import os
import pathlib
import tempfile
from collections import OrderedDict
from datetime import datetime, timedelta

import pytest
from flyteidl.core import compiler_pb2 as _compiler_pb2
from mock import MagicMock, patch

import flytekit.configuration
from flytekit import CronSchedule, LaunchPlan, task, workflow
from flytekit.configuration import Config, DefaultImages, ImageConfig
from flytekit.core.base_task import PythonTask
from flytekit.exceptions import user as user_exceptions
from flytekit.models import common as common_models
from flytekit.models import security
from flytekit.models.admin.workflow import Workflow, WorkflowClosure
from flytekit.models.core.compiler import CompiledWorkflowClosure
from flytekit.models.core.identifier import Identifier, ResourceType, WorkflowExecutionIdentifier
from flytekit.models.execution import Execution
from flytekit.models.task import Task
from flytekit.remote.lazy_entity import LazyEntity
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.translator import Options, get_serializable, get_serializable_launch_plan
from tests.flytekit.common.parameterizers import LIST_OF_TASK_CLOSURES

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
def remote():
    with patch("flytekit.clients.friendly.SynchronousFlyteClient") as mock_client:
        flyte_remote = FlyteRemote(config=Config.auto(), default_project="p1", default_domain="d1")
        flyte_remote._client_initialized = True
        flyte_remote._client = mock_client
        return flyte_remote


def test_remote_fetch_execution(remote):
    admin_workflow_execution = Execution(
        id=WorkflowExecutionIdentifier("p1", "d1", "n1"),
        spec=MagicMock(),
        closure=MagicMock(),
    )
    mock_client = MagicMock()
    mock_client.get_execution.return_value = admin_workflow_execution
    remote._client = mock_client
    flyte_workflow_execution = remote.fetch_execution(name="n1")
    assert flyte_workflow_execution.id == admin_workflow_execution.id


@pytest.fixture
def mock_wf_exec():
    return patch("flytekit.remote.executions.FlyteWorkflowExecution.promote_from_model")


def test_underscore_execute_uses_launch_plan_attributes(remote, mock_wf_exec):
    mock_wf_exec.return_value = True
    mock_client = MagicMock()
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


def test_underscore_execute_fall_back_remote_attributes(remote, mock_wf_exec):
    mock_wf_exec.return_value = True
    mock_client = MagicMock()
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


def test_execute_with_wrong_input_key(remote, mock_wf_exec):
    # mock_url.get.return_value = "localhost"
    # mock_insecure.get.return_value = True
    mock_wf_exec.return_value = True
    mock_client = MagicMock()
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
    FlyteRemote(config=Config.auto(), default_project="project", default_domain="domain", **additional_args).client
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
def test_generate_console_http_domain_sandbox_rewrite(mock_client):
    _, temp_filename = tempfile.mkstemp(suffix=".yaml")
    try:
        with open(temp_filename, "w") as f:
            # This string is similar to the relevant configuration emitted by flytectl in the cases of both demo and sandbox.
            flytectl_config_file = """admin:
    endpoint: example.com
    authType: Pkce
    insecure: false
"""
            f.write(flytectl_config_file)

        remote = FlyteRemote(
            config=Config.auto(config_file=temp_filename), default_project="project", default_domain="domain"
        )
        assert remote.generate_console_http_domain() == "https://example.com"

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
        assert remote.generate_console_http_domain() == "http://localhost:30081"

        with open(temp_filename, "w") as f:
            # This string is similar to the relevant configuration emitted by flytectl in the cases of both demo and sandbox.
            flytectl_config_file = """admin:
    endpoint: localhost:30081
    authType: Pkce
    insecure: true
console:
    endpoint: http://localhost:30090
"""
            f.write(flytectl_config_file)

        remote = FlyteRemote(
            config=Config.auto(config_file=temp_filename), default_project="project", default_domain="domain"
        )
        assert remote.generate_console_http_domain() == "http://localhost:30090"
    finally:
        try:
            os.remove(temp_filename)
        except OSError:
            pass


def get_compiled_workflow_closure():
    """
    :rtype: flytekit.models.core.compiler.CompiledWorkflowClosure
    """
    cwc_pb = _compiler_pb2.CompiledWorkflowClosure()
    # So that tests that use this work when run from any directory
    basepath = os.path.dirname(__file__)
    filepath = os.path.abspath(os.path.join(basepath, "responses", "CompiledWorkflowClosure.pb"))
    with open(filepath, "rb") as fh:
        cwc_pb.ParseFromString(fh.read())

    return CompiledWorkflowClosure.from_flyte_idl(cwc_pb)


def test_fetch_lazy(remote):
    mock_client = remote._client
    mock_client.get_task.return_value = Task(
        id=Identifier(ResourceType.TASK, "p", "d", "n", "v"), closure=LIST_OF_TASK_CLOSURES[0]
    )

    mock_client.get_workflow.return_value = Workflow(
        id=Identifier(ResourceType.TASK, "p", "d", "n", "v"),
        closure=WorkflowClosure(compiled_workflow=get_compiled_workflow_closure()),
    )

    lw = remote.fetch_workflow_lazy(name="wn", version="v")
    assert isinstance(lw, LazyEntity)
    assert lw._getter
    assert lw._entity is None
    assert lw.entity

    lt = remote.fetch_task_lazy(name="n", version="v")
    assert isinstance(lw, LazyEntity)
    assert lt._getter
    assert lt._entity is None
    tk = lt.entity
    assert tk.name == "n"


@task
def tk(t: datetime, v: int):
    print(f"Invoked at {t} with v {v}")


@workflow
def example_wf(t: datetime, v: int):
    tk(t=t, v=v)


def test_launch_backfill(remote):
    daily_lp = LaunchPlan.get_or_create(
        workflow=example_wf,
        name="daily2",
        fixed_inputs={"v": 10},
        schedule=CronSchedule(schedule="0 8 * * *", kickoff_time_input_arg="t"),
    )

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig.auto(img_name=DefaultImages.default_image()),
    )

    start_date = datetime(2022, 12, 1, 8)
    end_date = start_date + timedelta(days=10)

    ser_lp = get_serializable_launch_plan(OrderedDict(), serialization_settings, daily_lp, recurse_downstream=False)
    m = OrderedDict()
    ser_wf = get_serializable(m, serialization_settings, example_wf)
    tasks = []
    for k, v in m.items():
        if isinstance(k, PythonTask):
            tasks.append(v)
    mock_client = remote._client
    mock_client.get_launch_plan.return_value = ser_lp
    mock_client.get_workflow.return_value = Workflow(
        id=Identifier(ResourceType.WORKFLOW, "p", "d", "daily2", "v"),
        closure=WorkflowClosure(
            compiled_workflow=CompiledWorkflowClosure(primary=ser_wf, sub_workflows=[], tasks=tasks)
        ),
    )

    wf = remote.launch_backfill("p", "d", start_date, end_date, "daily2", "v1", dry_run=True)
    assert wf

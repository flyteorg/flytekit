from __future__ import absolute_import

import os

import pytest
from flyteidl.core import errors_pb2
from mock import MagicMock, PropertyMock, patch

from flytekit.common import constants, utils
from flytekit.common.exceptions import scopes
from flytekit.configuration import TemporaryConfiguration
from flytekit.engines.flyte import engine
from flytekit.models import common as _common_models
from flytekit.models import execution as _execution_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import literals
from flytekit.models import task as _task_models
from flytekit.models.admin import common as _common
from flytekit.models.core import errors, identifier
from flytekit.sdk import test_utils

_INPUT_MAP = literals.LiteralMap(
    {"a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1)))}
)
_OUTPUT_MAP = literals.LiteralMap(
    {"b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=2)))}
)
_EMPTY_LITERAL_MAP = literals.LiteralMap(literals={})


@pytest.fixture(scope="function", autouse=True)
def temp_config():
    with TemporaryConfiguration(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "../../../common/configs/local.config",
        ),
        internal_overrides={
            "image": "myflyteimage:{}".format(os.environ.get("IMAGE_VERSION", "sha")),
            "project": "myflyteproject",
            "domain": "development",
        },
    ):
        yield


@pytest.fixture(scope="function", autouse=True)
def execution_data_locations():
    with test_utils.LocalTestFileSystem() as fs:
        input_filename = fs.get_named_tempfile("inputs.pb")
        output_filename = fs.get_named_tempfile("outputs.pb")
        utils.write_proto_to_file(_INPUT_MAP.to_flyte_idl(), input_filename)
        utils.write_proto_to_file(_OUTPUT_MAP.to_flyte_idl(), output_filename)
        yield (
            _common_models.UrlBlob(input_filename, 100),
            _common_models.UrlBlob(output_filename, 100),
        )


@scopes.system_entry_point
def _raise_system_exception(*args, **kwargs):
    raise ValueError("errorERRORerror")


@scopes.user_entry_point
def _raise_user_exception(*args, **kwargs):
    raise ValueError("userUSERuser")


@scopes.system_entry_point
def test_task_system_failure():
    m = MagicMock()
    m.execute = _raise_system_exception

    with utils.AutoDeletingTempDir("test") as tmp:
        engine.FlyteTask(m).execute(None, {"output_prefix": tmp.name})

        doc = errors.ErrorDocument.from_flyte_idl(
            utils.load_proto_from_file(
                errors_pb2.ErrorDocument,
                os.path.join(tmp.name, constants.ERROR_FILE_NAME),
            )
        )
        assert doc.error.code == "SYSTEM:Unknown"
        assert doc.error.kind == errors.ContainerError.Kind.RECOVERABLE
        assert "errorERRORerror" in doc.error.message


@scopes.system_entry_point
def test_task_user_failure():
    m = MagicMock()
    m.execute = _raise_user_exception

    with utils.AutoDeletingTempDir("test") as tmp:
        engine.FlyteTask(m).execute(None, {"output_prefix": tmp.name})

        doc = errors.ErrorDocument.from_flyte_idl(
            utils.load_proto_from_file(
                errors_pb2.ErrorDocument,
                os.path.join(tmp.name, constants.ERROR_FILE_NAME),
            )
        )
        assert doc.error.code == "USER:Unknown"
        assert doc.error.kind == errors.ContainerError.Kind.NON_RECOVERABLE
        assert "userUSERuser" in doc.error.message


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_execution_notification_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier("xp", "xd", "xn"))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version")
    )

    engine.FlyteLaunchPlan(m).launch("xp", "xd", "xn", literals.LiteralMap({}), notification_overrides=[])

    mock_client.create_execution.assert_called_once_with(
        "xp",
        "xd",
        "xn",
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version",
            ),
            _execution_models.ExecutionMetadata(_execution_models.ExecutionMetadata.ExecutionMode.MANUAL, "sdk", 0),
            disable_all=True,
        ),
        literals.LiteralMap({}),
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_execution_notification_soft_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier("xp", "xd", "xn"))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version")
    )

    notification = _common_models.Notification([0, 1, 2], email=_common_models.EmailNotification(["me@place.com"]))

    engine.FlyteLaunchPlan(m).launch("xp", "xd", "xn", literals.LiteralMap({}), notification_overrides=[notification])

    mock_client.create_execution.assert_called_once_with(
        "xp",
        "xd",
        "xn",
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version",
            ),
            _execution_models.ExecutionMetadata(_execution_models.ExecutionMetadata.ExecutionMode.MANUAL, "sdk", 0),
            notifications=_execution_models.NotificationList([notification]),
        ),
        literals.LiteralMap({}),
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_execution_label_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier("xp", "xd", "xn"))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version")
    )

    labels = _common_models.Labels({"my": "label"})
    engine.FlyteLaunchPlan(m).execute(
        "xp",
        "xd",
        "xn",
        literals.LiteralMap({}),
        notification_overrides=[],
        label_overrides=labels,
    )

    mock_client.create_execution.assert_called_once_with(
        "xp",
        "xd",
        "xn",
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version",
            ),
            _execution_models.ExecutionMetadata(_execution_models.ExecutionMetadata.ExecutionMode.MANUAL, "sdk", 0),
            disable_all=True,
            labels=labels,
        ),
        literals.LiteralMap({}),
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_execution_annotation_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier("xp", "xd", "xn"))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version")
    )

    annotations = _common_models.Annotations({"my": "annotation"})
    engine.FlyteLaunchPlan(m).launch(
        "xp",
        "xd",
        "xn",
        literals.LiteralMap({}),
        notification_overrides=[],
        annotation_overrides=annotations,
    )

    mock_client.create_execution.assert_called_once_with(
        "xp",
        "xd",
        "xn",
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version",
            ),
            _execution_models.ExecutionMetadata(_execution_models.ExecutionMetadata.ExecutionMode.MANUAL, "sdk", 0),
            disable_all=True,
            annotations=annotations,
        ),
        literals.LiteralMap({}),
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_fetch_launch_plan(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_launch_plan = MagicMock(
        return_value=_launch_plan_models.LaunchPlan(
            identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1"),
            MagicMock(),
            MagicMock(),
        )
    )
    mock_client_factory.return_value = mock_client

    lp = engine.FlyteEngineFactory().fetch_launch_plan(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p", "d", "n", "v")
    )
    assert lp.id == identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1")

    mock_client.get_launch_plan.assert_called_once_with(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p", "d", "n", "v")
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_fetch_active_launch_plan(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_active_launch_plan = MagicMock(
        return_value=_launch_plan_models.LaunchPlan(
            identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1"),
            MagicMock(),
            MagicMock(),
        )
    )
    mock_client_factory.return_value = mock_client

    lp = engine.FlyteEngineFactory().fetch_launch_plan(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p", "d", "n", "")
    )
    assert lp.id == identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1")

    mock_client.get_active_launch_plan.assert_called_once_with(_common_models.NamedEntityIdentifier("p", "d", "n"))


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_full_execution_inputs(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_execution_data = MagicMock(
        return_value=_execution_models.WorkflowExecutionGetDataResponse(
            None,
            None,
            _INPUT_MAP,
            _OUTPUT_MAP,
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.WorkflowExecutionIdentifier(
            "project",
            "domain",
            "name",
        )
    )

    inputs = engine.FlyteWorkflowExecution(m).get_inputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["a"].scalar.primitive.integer == 1
    mock_client.get_execution_data.assert_called_once_with(
        identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_execution_inputs(mock_client_factory, execution_data_locations):
    mock_client = MagicMock()
    mock_client.get_execution_data = MagicMock(
        return_value=_execution_models.WorkflowExecutionGetDataResponse(
            execution_data_locations[0], execution_data_locations[1], _EMPTY_LITERAL_MAP, _EMPTY_LITERAL_MAP
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.WorkflowExecutionIdentifier(
            "project",
            "domain",
            "name",
        )
    )

    inputs = engine.FlyteWorkflowExecution(m).get_inputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["a"].scalar.primitive.integer == 1
    mock_client.get_execution_data.assert_called_once_with(
        identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_full_execution_outputs(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_execution_data = MagicMock(
        return_value=_execution_models.WorkflowExecutionGetDataResponse(None, None, _INPUT_MAP, _OUTPUT_MAP)
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.WorkflowExecutionIdentifier(
            "project",
            "domain",
            "name",
        )
    )

    outputs = engine.FlyteWorkflowExecution(m).get_outputs()
    assert len(outputs.literals) == 1
    assert outputs.literals["b"].scalar.primitive.integer == 2
    mock_client.get_execution_data.assert_called_once_with(
        identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_execution_outputs(mock_client_factory, execution_data_locations):
    mock_client = MagicMock()
    mock_client.get_execution_data = MagicMock(
        return_value=_execution_models.WorkflowExecutionGetDataResponse(
            execution_data_locations[0], execution_data_locations[1], _EMPTY_LITERAL_MAP, _EMPTY_LITERAL_MAP
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.WorkflowExecutionIdentifier(
            "project",
            "domain",
            "name",
        )
    )

    inputs = engine.FlyteWorkflowExecution(m).get_outputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["b"].scalar.primitive.integer == 2
    mock_client.get_execution_data.assert_called_once_with(
        identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_full_node_execution_inputs(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_node_execution_data = MagicMock(
        return_value=_execution_models.NodeExecutionGetDataResponse(
            None,
            None,
            _INPUT_MAP,
            _OUTPUT_MAP,
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )

    inputs = engine.FlyteNodeExecution(m).get_inputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["a"].scalar.primitive.integer == 1
    mock_client.get_node_execution_data.assert_called_once_with(
        identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_node_execution_inputs(mock_client_factory, execution_data_locations):
    mock_client = MagicMock()
    mock_client.get_node_execution_data = MagicMock(
        return_value=_execution_models.NodeExecutionGetDataResponse(
            execution_data_locations[0], execution_data_locations[1], _EMPTY_LITERAL_MAP, _EMPTY_LITERAL_MAP
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )

    inputs = engine.FlyteNodeExecution(m).get_inputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["a"].scalar.primitive.integer == 1
    mock_client.get_node_execution_data.assert_called_once_with(
        identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_full_node_execution_outputs(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_node_execution_data = MagicMock(
        return_value=_execution_models.NodeExecutionGetDataResponse(None, None, _INPUT_MAP, _OUTPUT_MAP)
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )

    outputs = engine.FlyteNodeExecution(m).get_outputs()
    assert len(outputs.literals) == 1
    assert outputs.literals["b"].scalar.primitive.integer == 2
    mock_client.get_node_execution_data.assert_called_once_with(
        identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_node_execution_outputs(mock_client_factory, execution_data_locations):
    mock_client = MagicMock()
    mock_client.get_node_execution_data = MagicMock(
        return_value=_execution_models.NodeExecutionGetDataResponse(
            execution_data_locations[0], execution_data_locations[1], _EMPTY_LITERAL_MAP, _EMPTY_LITERAL_MAP
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )

    inputs = engine.FlyteNodeExecution(m).get_outputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["b"].scalar.primitive.integer == 2
    mock_client.get_node_execution_data.assert_called_once_with(
        identifier.NodeExecutionIdentifier(
            "node-a",
            identifier.WorkflowExecutionIdentifier(
                "project",
                "domain",
                "name",
            ),
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_full_task_execution_inputs(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_task_execution_data = MagicMock(
        return_value=_execution_models.TaskExecutionGetDataResponse(None, None, _INPUT_MAP, _OUTPUT_MAP)
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )

    inputs = engine.FlyteTaskExecution(m).get_inputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["a"].scalar.primitive.integer == 1
    mock_client.get_task_execution_data.assert_called_once_with(
        identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_task_execution_inputs(mock_client_factory, execution_data_locations):
    mock_client = MagicMock()
    mock_client.get_task_execution_data = MagicMock(
        return_value=_execution_models.TaskExecutionGetDataResponse(
            execution_data_locations[0], execution_data_locations[1], _EMPTY_LITERAL_MAP, _EMPTY_LITERAL_MAP
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )

    inputs = engine.FlyteTaskExecution(m).get_inputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["a"].scalar.primitive.integer == 1
    mock_client.get_task_execution_data.assert_called_once_with(
        identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_full_task_execution_outputs(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_task_execution_data = MagicMock(
        return_value=_execution_models.TaskExecutionGetDataResponse(None, None, _INPUT_MAP, _OUTPUT_MAP)
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )

    outputs = engine.FlyteTaskExecution(m).get_outputs()
    assert len(outputs.literals) == 1
    assert outputs.literals["b"].scalar.primitive.integer == 2
    mock_client.get_task_execution_data.assert_called_once_with(
        identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )


@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_get_task_execution_outputs(mock_client_factory, execution_data_locations):
    mock_client = MagicMock()
    mock_client.get_task_execution_data = MagicMock(
        return_value=_execution_models.TaskExecutionGetDataResponse(
            execution_data_locations[0], execution_data_locations[1], _EMPTY_LITERAL_MAP, _EMPTY_LITERAL_MAP
        )
    )
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )

    inputs = engine.FlyteTaskExecution(m).get_outputs()
    assert len(inputs.literals) == 1
    assert inputs.literals["b"].scalar.primitive.integer == 2
    mock_client.get_task_execution_data.assert_called_once_with(
        identifier.TaskExecutionIdentifier(
            identifier.Identifier(
                identifier.ResourceType.TASK,
                "project",
                "domain",
                "task-name",
                "version",
            ),
            identifier.NodeExecutionIdentifier(
                "node-a",
                identifier.WorkflowExecutionIdentifier(
                    "project",
                    "domain",
                    "name",
                ),
            ),
            0,
        )
    )


@pytest.mark.parametrize(
    "tasks",
    [
        [
            _task_models.Task(
                identifier.Identifier(identifier.ResourceType.TASK, "p1", "d1", "n1", "v1"),
                MagicMock(),
            )
        ],
        [],
    ],
)
@patch.object(engine._FlyteClientManager, "_CLIENT", new_callable=PropertyMock)
def test_fetch_latest_task(mock_client_factory, tasks):
    mock_client = MagicMock()
    mock_client.list_tasks_paginated = MagicMock(return_value=(tasks, 0))
    mock_client_factory.return_value = mock_client

    task = engine.FlyteEngineFactory().fetch_latest_task(_common_models.NamedEntityIdentifier("p", "d", "n"))

    if tasks:
        assert task.id == tasks[0].id
    else:
        assert not task

    mock_client.list_tasks_paginated.assert_called_once_with(
        _common_models.NamedEntityIdentifier("p", "d", "n"),
        limit=1,
        sort_by=_common.Sort("created_at", _common.Sort.Direction.DESCENDING),
    )

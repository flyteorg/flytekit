import pytest

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.control_plane import identifier as _identifier
from flytekit.models.core import identifier as _core_identifier


def test_identifier():
    identifier = _identifier.Identifier(_core_identifier.ResourceType.WORKFLOW, "project", "domain", "name", "v1")
    assert identifier == _identifier.Identifier.from_urn("wf:project:domain:name:v1")
    assert identifier == _core_identifier.Identifier(
        _core_identifier.ResourceType.WORKFLOW, "project", "domain", "name", "v1"
    )
    assert identifier.__str__() == "wf:project:domain:name:v1"


@pytest.mark.parametrize(
    "urn",
    [
        "",
        "project:domain:name:v1",
        "wf:project:domain:name:v1:foobar",
        "foobar:project:domain:name:v1",
    ],
)
def test_identifier_exceptions(urn):
    with pytest.raises(_user_exceptions.FlyteValueException):
        _identifier.Identifier.from_urn(urn)


def test_workflow_execution_identifier():
    identifier = _identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    assert identifier == _identifier.WorkflowExecutionIdentifier.from_urn("ex:project:domain:name")
    assert identifier == _identifier.WorkflowExecutionIdentifier.promote_from_model(
        _core_identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    )
    assert identifier.__str__() == "ex:project:domain:name"


@pytest.mark.parametrize(
    "urn", ["", "project:domain:name", "project:domain:name:foobar", "ex:project:domain:name:foobar"]
)
def test_workflow_execution_identifier_exceptions(urn):
    with pytest.raises(_user_exceptions.FlyteValueException):
        _identifier.WorkflowExecutionIdentifier.from_urn(urn)


def test_task_execution_identifier():
    task_id = _identifier.Identifier(_core_identifier.ResourceType.TASK, "project", "domain", "name", "version")
    node_execution_id = _core_identifier.NodeExecutionIdentifier(
        node_id="n0", execution_id=_core_identifier.WorkflowExecutionIdentifier("project", "domain", "name")
    )
    identifier = _identifier.TaskExecutionIdentifier(
        task_id=task_id,
        node_execution_id=node_execution_id,
        retry_attempt=0,
    )
    assert identifier == _identifier.TaskExecutionIdentifier.from_urn(
        "te:project:domain:name:n0:project:domain:name:version:0"
    )
    assert identifier == _identifier.TaskExecutionIdentifier.promote_from_model(
        _core_identifier.TaskExecutionIdentifier(task_id, node_execution_id, 0)
    )
    assert identifier.__str__() == "te:project:domain:name:n0:project:domain:name:version:0"


@pytest.mark.parametrize(
    "urn",
    [
        "",
        "te:project:domain:name:n0:project:domain:name:version",
        "foobar:project:domain:name:n0:project:domain:name:version:0",
    ],
)
def test_task_execution_identifier_exceptions(urn):
    with pytest.raises(_user_exceptions.FlyteValueException):
        _identifier.TaskExecutionIdentifier.from_urn(urn)

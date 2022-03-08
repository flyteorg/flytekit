import datetime

import pytest
import pytz

from flytekit.models import common as _common_models
from flytekit.models import execution as _execution
from flytekit.models import literals as _literals
from flytekit.models.core import execution as _core_exec
from flytekit.models.core import identifier as _identifier
from tests.flytekit.common import parameterizers as _parameterizers

_INPUT_MAP = _literals.LiteralMap(
    {"a": _literals.Literal(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=1)))}
)
_OUTPUT_MAP = _literals.LiteralMap(
    {"b": _literals.Literal(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=2)))}
)


def test_execution_closure_with_output():
    test_datetime = datetime.datetime(year=2022, month=1, day=1, tzinfo=pytz.UTC)
    test_timedelta = datetime.timedelta(seconds=10)
    test_outputs = _execution.LiteralMapBlob(values=_OUTPUT_MAP, uri="http://foo/")

    obj = _execution.ExecutionClosure(
        phase=_core_exec.WorkflowExecutionPhase.SUCCEEDED,
        started_at=test_datetime,
        duration=test_timedelta,
        outputs=test_outputs,
    )
    assert obj.phase == _core_exec.WorkflowExecutionPhase.SUCCEEDED
    assert obj.started_at == test_datetime
    assert obj.duration == test_timedelta
    assert obj.outputs == test_outputs
    obj2 = _execution.ExecutionClosure.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj
    assert obj2.phase == _core_exec.WorkflowExecutionPhase.SUCCEEDED
    assert obj2.started_at == test_datetime
    assert obj2.duration == test_timedelta
    assert obj2.outputs == test_outputs


def test_execution_closure_with_error():
    test_datetime = datetime.datetime(year=2022, month=1, day=1, tzinfo=pytz.UTC)
    test_timedelta = datetime.timedelta(seconds=10)
    test_error = _core_exec.ExecutionError(
        code="foo", message="bar", error_uri="http://foobar", kind=_core_exec.ExecutionError.ErrorKind.USER
    )

    obj = _execution.ExecutionClosure(
        phase=_core_exec.WorkflowExecutionPhase.SUCCEEDED,
        started_at=test_datetime,
        duration=test_timedelta,
        error=test_error,
    )
    assert obj.phase == _core_exec.WorkflowExecutionPhase.SUCCEEDED
    assert obj.started_at == test_datetime
    assert obj.duration == test_timedelta
    assert obj.error == test_error
    obj2 = _execution.ExecutionClosure.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj
    assert obj2.phase == _core_exec.WorkflowExecutionPhase.SUCCEEDED
    assert obj2.started_at == test_datetime
    assert obj2.duration == test_timedelta
    assert obj2.error == test_error


def test_execution_metadata():
    obj = _execution.ExecutionMetadata(_execution.ExecutionMetadata.ExecutionMode.MANUAL, "tester", 1)
    assert obj.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj.principal == "tester"
    assert obj.nesting == 1
    obj2 = _execution.ExecutionMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj2.principal == "tester"
    assert obj2.nesting == 1


@pytest.mark.parametrize("literal_value_pair", _parameterizers.LIST_OF_SCALAR_LITERALS_AND_PYTHON_VALUE)
def test_execution_spec(literal_value_pair):
    literal_value, _ = literal_value_pair

    obj = _execution.ExecutionSpec(
        _identifier.Identifier(_identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version"),
        _execution.ExecutionMetadata(_execution.ExecutionMetadata.ExecutionMode.MANUAL, "tester", 1),
        notifications=_execution.NotificationList(
            [
                _common_models.Notification(
                    [_core_exec.WorkflowExecutionPhase.ABORTED],
                    pager_duty=_common_models.PagerDutyNotification(recipients_email=["a", "b", "c"]),
                )
            ]
        ),
        max_parallelism=100,
    )
    assert obj.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj.launch_plan.domain == "domain"
    assert obj.launch_plan.project == "project"
    assert obj.launch_plan.name == "name"
    assert obj.launch_plan.version == "version"
    assert obj.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj.metadata.nesting == 1
    assert obj.metadata.principal == "tester"
    assert obj.notifications.notifications[0].phases == [_core_exec.WorkflowExecutionPhase.ABORTED]
    assert obj.notifications.notifications[0].pager_duty.recipients_email == [
        "a",
        "b",
        "c",
    ]
    assert obj.disable_all is None
    assert obj.max_parallelism == 100

    obj2 = _execution.ExecutionSpec.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj2.launch_plan.domain == "domain"
    assert obj2.launch_plan.project == "project"
    assert obj2.launch_plan.name == "name"
    assert obj2.launch_plan.version == "version"
    assert obj2.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj2.metadata.nesting == 1
    assert obj2.metadata.principal == "tester"
    assert obj2.notifications.notifications[0].phases == [_core_exec.WorkflowExecutionPhase.ABORTED]
    assert obj2.notifications.notifications[0].pager_duty.recipients_email == [
        "a",
        "b",
        "c",
    ]
    assert obj2.disable_all is None
    assert obj2.max_parallelism == 100

    obj = _execution.ExecutionSpec(
        _identifier.Identifier(_identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version"),
        _execution.ExecutionMetadata(_execution.ExecutionMetadata.ExecutionMode.MANUAL, "tester", 1),
        disable_all=True,
    )
    assert obj.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj.launch_plan.domain == "domain"
    assert obj.launch_plan.project == "project"
    assert obj.launch_plan.name == "name"
    assert obj.launch_plan.version == "version"
    assert obj.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj.metadata.nesting == 1
    assert obj.metadata.principal == "tester"
    assert obj.notifications is None
    assert obj.disable_all is True

    obj2 = _execution.ExecutionSpec.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj2.launch_plan.domain == "domain"
    assert obj2.launch_plan.project == "project"
    assert obj2.launch_plan.name == "name"
    assert obj2.launch_plan.version == "version"
    assert obj2.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj2.metadata.nesting == 1
    assert obj2.metadata.principal == "tester"
    assert obj2.notifications is None
    assert obj2.disable_all is True


def test_workflow_execution_data_response():
    input_blob = _common_models.UrlBlob("in", 1)
    output_blob = _common_models.UrlBlob("out", 2)
    obj = _execution.WorkflowExecutionGetDataResponse(input_blob, output_blob, _INPUT_MAP, _OUTPUT_MAP)
    obj2 = _execution.WorkflowExecutionGetDataResponse.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.inputs == input_blob
    assert obj2.outputs == output_blob
    assert obj2.full_inputs == _INPUT_MAP
    assert obj2.full_outputs == _OUTPUT_MAP


def test_node_execution_data_response():
    input_blob = _common_models.UrlBlob("in", 1)
    output_blob = _common_models.UrlBlob("out", 2)
    obj = _execution.NodeExecutionGetDataResponse(input_blob, output_blob, _INPUT_MAP, _OUTPUT_MAP)
    obj2 = _execution.NodeExecutionGetDataResponse.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.inputs == input_blob
    assert obj2.outputs == output_blob
    assert obj2.full_inputs == _INPUT_MAP
    assert obj2.full_outputs == _OUTPUT_MAP


def test_task_execution_data_response():
    input_blob = _common_models.UrlBlob("in", 1)
    output_blob = _common_models.UrlBlob("out", 2)
    obj = _execution.TaskExecutionGetDataResponse(input_blob, output_blob, _INPUT_MAP, _OUTPUT_MAP)
    obj2 = _execution.TaskExecutionGetDataResponse.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.inputs == input_blob
    assert obj2.outputs == output_blob
    assert obj2.full_inputs == _INPUT_MAP
    assert obj2.full_outputs == _OUTPUT_MAP

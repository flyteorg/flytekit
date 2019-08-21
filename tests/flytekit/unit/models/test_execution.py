from __future__ import absolute_import
from flytekit.models import execution as _execution, literals as _literal_models, common as _common_models
from flytekit.models.core import execution as _core_exec, identifier as _identifier
from tests.flytekit.common import parameterizers as _parameterizers
import pytest


def test_execution_metadata():
    obj = _execution.ExecutionMetadata(_execution.ExecutionMetadata.ExecutionMode.MANUAL, 'tester', 1)
    assert obj.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj.principal == 'tester'
    assert obj.nesting == 1
    obj2 = _execution.ExecutionMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj2.principal == 'tester'
    assert obj2.nesting == 1


@pytest.mark.parametrize(
    "literal_value_pair",
    _parameterizers.LIST_OF_SCALAR_LITERALS_AND_PYTHON_VALUE
)
def test_execution_spec(literal_value_pair):
    literal_value, _ = literal_value_pair

    obj = _execution.ExecutionSpec(
        _identifier.Identifier(_identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version"),
        _literal_models.LiteralMap(literals={'a': literal_value}),
        _execution.ExecutionMetadata(_execution.ExecutionMetadata.ExecutionMode.MANUAL, 'tester', 1),
        notifications=_execution.NotificationList(
            [
                _common_models.Notification(
                    [_core_exec.WorkflowExecutionPhase.ABORTED],
                    pager_duty=_common_models.PagerDutyNotification(recipients_email=['a', 'b', 'c'])
                )
            ]
        )
    )
    assert obj.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj.launch_plan.domain == "domain"
    assert obj.launch_plan.project == "project"
    assert obj.launch_plan.name == "name"
    assert obj.launch_plan.version == "version"
    assert obj.inputs.literals['a'] == literal_value
    assert obj.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj.metadata.nesting == 1
    assert obj.metadata.principal == 'tester'
    assert obj.notifications.notifications[0].phases == [_core_exec.WorkflowExecutionPhase.ABORTED]
    assert obj.notifications.notifications[0].pager_duty.recipients_email == ['a', 'b', 'c']
    assert obj.disable_all is None

    obj2 = _execution.ExecutionSpec.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj2.launch_plan.domain == "domain"
    assert obj2.launch_plan.project == "project"
    assert obj2.launch_plan.name == "name"
    assert obj2.launch_plan.version == "version"
    assert obj2.inputs.literals['a'] == literal_value
    assert obj2.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj2.metadata.nesting == 1
    assert obj2.metadata.principal == 'tester'
    assert obj2.notifications.notifications[0].phases == [_core_exec.WorkflowExecutionPhase.ABORTED]
    assert obj2.notifications.notifications[0].pager_duty.recipients_email == ['a', 'b', 'c']
    assert obj2.disable_all is None

    obj = _execution.ExecutionSpec(
        _identifier.Identifier(_identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version"),
        _literal_models.LiteralMap(literals={'a': literal_value}),
        _execution.ExecutionMetadata(_execution.ExecutionMetadata.ExecutionMode.MANUAL, 'tester', 1),
        disable_all=True
    )
    assert obj.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj.launch_plan.domain == "domain"
    assert obj.launch_plan.project == "project"
    assert obj.launch_plan.name == "name"
    assert obj.launch_plan.version == "version"
    assert obj.inputs.literals['a'] == literal_value
    assert obj.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj.metadata.nesting == 1
    assert obj.metadata.principal == 'tester'
    assert obj.notifications is None
    assert obj.disable_all is True

    obj2 = _execution.ExecutionSpec.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.launch_plan.resource_type == _identifier.ResourceType.LAUNCH_PLAN
    assert obj2.launch_plan.domain == "domain"
    assert obj2.launch_plan.project == "project"
    assert obj2.launch_plan.name == "name"
    assert obj2.launch_plan.version == "version"
    assert obj2.inputs.literals['a'] == literal_value
    assert obj2.metadata.mode == _execution.ExecutionMetadata.ExecutionMode.MANUAL
    assert obj2.metadata.nesting == 1
    assert obj2.metadata.principal == 'tester'
    assert obj2.notifications is None
    assert obj2.disable_all is True

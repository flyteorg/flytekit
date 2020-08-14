from __future__ import absolute_import

from flytekit.common import notifications as _notifications
from flytekit.models.core import execution as _execution_model


def test_pager_duty():
    obj = _notifications.PagerDuty(
        [_execution_model.WorkflowExecutionPhase.FAILED], ["me@myplace.com"]
    )
    assert obj.email is None
    assert obj.slack is None
    assert obj.phases == [_execution_model.WorkflowExecutionPhase.FAILED]
    assert obj.pager_duty.recipients_email == ["me@myplace.com"]

    obj2 = _notifications.PagerDuty.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_slack():
    obj = _notifications.Slack(
        [_execution_model.WorkflowExecutionPhase.FAILED], ["me@myplace.com"]
    )
    assert obj.email is None
    assert obj.pager_duty is None
    assert obj.phases == [_execution_model.WorkflowExecutionPhase.FAILED]
    assert obj.slack.recipients_email == ["me@myplace.com"]

    obj2 = _notifications.Slack.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_email():
    obj = _notifications.Email(
        [_execution_model.WorkflowExecutionPhase.FAILED], ["me@myplace.com"]
    )
    assert obj.pager_duty is None
    assert obj.slack is None
    assert obj.phases == [_execution_model.WorkflowExecutionPhase.FAILED]
    assert obj.email.recipients_email == ["me@myplace.com"]

    obj2 = _notifications.Email.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2

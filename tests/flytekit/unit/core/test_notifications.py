import json
import flyteidl_rust as flyteidl

from flytekit.core import notification
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.models import common as _common_model
from flytekit.models.core import execution as _execution_model

_workflow_execution_succeeded = _execution_model.WorkflowExecutionPhase.SUCCEEDED


def test_pager_duty_notification():
    pager_duty_notif = notification.PagerDuty(
        phases=[_workflow_execution_succeeded], recipients_email=["my-team@pagerduty.com"]
    )
    assert json.loads(pager_duty_notif.to_flyte_idl().DumpToJsonString()) == json.loads(flyteidl.admin.Notification(
        phases=[_workflow_execution_succeeded],
        type=flyteidl.notification.Type.PagerDuty(_common_model.PagerDutyNotification(["my-team@pagerduty.com"]).to_flyte_idl()),
    ).DumpToJsonString())


def test_slack_notification():
    slack_notif = notification.Slack(phases=[_workflow_execution_succeeded], recipients_email=["my-team@slack.com"])
    assert json.loads(slack_notif.to_flyte_idl().DumpToJsonString()) == json.loads(flyteidl.admin.Notification(
        phases=[_workflow_execution_succeeded],
        type=flyteidl.notification.Type.Slack(_common_model.SlackNotification(["my-team@slack.com"]).to_flyte_idl()),
    ).DumpToJsonString())


def test_email_notification():
    email_notif = notification.Email(phases=[_workflow_execution_succeeded], recipients_email=["my-team@email.com"])
    assert json.loads(email_notif.to_flyte_idl().DumpToJsonString()) == json.loads(flyteidl.admin.Notification(
        phases=[_workflow_execution_succeeded],
        type=flyteidl.notification.Type.Email(_common_model.EmailNotification(["my-team@email.com"]).to_flyte_idl()),
    ).DumpToJsonString())


def test_with_launch_plan():
    @task
    def double(a: int) -> int:
        return a * 2

    @workflow
    def quadruple(a: int) -> int:
        b = double(a=a)
        c = double(a=b)
        return c

    lp = LaunchPlan.create(
        "notif_test",
        quadruple,
        notifications=[
            notification.Email(phases=[_workflow_execution_succeeded], recipients_email=["my-team@email.com"])
        ],
    )
    assert lp.notifications == [
        notification.Email(phases=[_workflow_execution_succeeded], recipients_email=["my-team@email.com"])
    ]

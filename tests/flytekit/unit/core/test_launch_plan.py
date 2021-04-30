import typing

import pytest
from flyteidl.admin import launch_plan_pb2 as _launch_plan_idl

from flytekit.core import launch_plan, notification
from flytekit.core.schedule import CronSchedule
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.models.common import AuthRole
from flytekit.models.core import execution as _execution_model


def test_lp():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow
    def wf(a: int) -> (str, str):
        x, y = t1(a=a)
        u, v = t1(a=x)
        return y, v

    lp = launch_plan.LaunchPlan.get_or_create(wf, "get_or_create1")
    lp2 = launch_plan.LaunchPlan.get_or_create(wf, "get_or_create1")
    assert lp.name == "get_or_create1"
    assert lp is lp2

    default_lp = launch_plan.LaunchPlan.get_or_create(wf)
    default_lp2 = launch_plan.LaunchPlan.get_or_create(wf)
    assert default_lp is default_lp2

    with pytest.raises(ValueError):
        launch_plan.LaunchPlan.get_or_create(wf, default_inputs={"a": 3})

    lp_with_defaults = launch_plan.LaunchPlan.create("get_or_create2", wf, default_inputs={"a": 3})
    assert lp_with_defaults.parameters.parameters["a"].default.scalar.primitive.integer == 3


def test_lp_each_parameter():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow
    def wf(a: int, c: str) -> (int, str):
        x, y = t1(a=a)
        return x, y

    # Fixed Inputs Parameter
    fixed_lp = launch_plan.LaunchPlan.get_or_create(
        workflow=wf, name="get_or_create_fixed", fixed_inputs={"a": 1, "c": "4"}
    )
    fixed_lp1 = launch_plan.LaunchPlan.get_or_create(workflow=wf, name="get_or_create_fixed")

    with pytest.raises(AssertionError):
        assert fixed_lp is fixed_lp1

    # Schedule Parameter
    obj = CronSchedule("* * ? * * *", kickoff_time_input_arg="abc")
    schedule_lp = launch_plan.LaunchPlan.get_or_create(workflow=wf, name="get_or_create_schedule", schedule=obj)
    schedule_lp1 = launch_plan.LaunchPlan.get_or_create(workflow=wf, name="get_or_create_schedule", schedule=obj)

    assert schedule_lp is schedule_lp1

    # Default Inputs Parameter
    default_lp = launch_plan.LaunchPlan.get_or_create(
        workflow=wf, name="get_or_create_schedule", default_inputs={"a": 9}
    )
    default_lp1 = launch_plan.LaunchPlan.get_or_create(
        workflow=wf, name="get_or_create_schedule", default_inputs={"a": 19}
    )

    # Validates both schedule and default inputs owing to the same launch plan
    with pytest.raises(AssertionError):
        assert default_lp is default_lp1

    # Notifications Parameter
    email_notif = notification.Email(
        phases=[_execution_model.WorkflowExecutionPhase.SUCCEEDED], recipients_email=["my-team@email.com"]
    )
    notification_lp = launch_plan.LaunchPlan.get_or_create(
        workflow=wf, name="get_or_create_notification", notifications=email_notif
    )
    notification_lp1 = launch_plan.LaunchPlan.get_or_create(
        workflow=wf, name="get_or_create_notification", notifications=email_notif
    )

    assert notification_lp is notification_lp1

    # Auth Parameter
    auth_role_model1 = AuthRole(assumable_iam_role="my:iam:role")
    auth_role_model2 = _launch_plan_idl.Auth(kubernetes_service_account="my:service:account")
    auth_lp = launch_plan.LaunchPlan.get_or_create(workflow=wf, name="get_or_create_auth", auth_role=auth_role_model1)
    auth_lp1 = launch_plan.LaunchPlan.get_or_create(workflow=wf, name="get_or_create_auth", auth_role=auth_role_model2)

    with pytest.raises(AssertionError):
        assert auth_lp is auth_lp1

    # Default LaunchPlan
    name_lp = launch_plan.LaunchPlan.get_or_create(workflow=wf)
    name_lp1 = launch_plan.LaunchPlan.get_or_create(workflow=wf)

    assert name_lp is name_lp1


def test_lp_all_parameters():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @task
    def t2(a: str, b: str, c: str) -> str:
        return b + a + c

    @workflow
    def wf(a: int, c: str) -> str:
        x, y = t1(a=a)
        u = t2(a=x, b=y, c=c)
        return u

    obj = CronSchedule("* * ? * * *", kickoff_time_input_arg="abc")
    obj1 = CronSchedule("10 * ? * * *", kickoff_time_input_arg="abc")
    slack_notif = notification.Slack(
        phases=[_execution_model.WorkflowExecutionPhase.SUCCEEDED], recipients_email=["my-team@slack.com"]
    )
    auth_role_model = AuthRole(assumable_iam_role="my:iam:role")

    lp = launch_plan.LaunchPlan.get_or_create(
        workflow=wf,
        name="get_or_create",
        default_inputs={"a": 3},
        fixed_inputs={"c": "4"},
        schedule=obj,
        notifications=slack_notif,
        auth_role=auth_role_model,
    )
    lp2 = launch_plan.LaunchPlan.get_or_create(
        workflow=wf,
        name="get_or_create",
        default_inputs={"a": 3},
        fixed_inputs={"c": "4"},
        schedule=obj,
        notifications=slack_notif,
        auth_role=auth_role_model,
    )

    assert lp is lp2

    # Check for assertion error when a different scheduler is used
    lp3 = launch_plan.LaunchPlan.get_or_create(
        workflow=wf,
        name="get_or_create",
        default_inputs={"a": 3},
        fixed_inputs={"c": "4"},
        schedule=obj1,
        notifications=slack_notif,
        auth_role=auth_role_model,
    )

    with pytest.raises(AssertionError):
        assert lp is lp3

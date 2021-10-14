import os as _os

import pytest as _pytest

from flytekit import configuration as _configuration
from flytekit.common import launch_plan as _launch_plan
from flytekit.common import notifications as _notifications
from flytekit.common import schedules as _schedules
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import common as _common_models
from flytekit.models.admin import schedule as _schedule
from flytekit.models import types as _type_models
from flytekit.models.core import execution as _execution
from flytekit.models.core import identifier as _identifier
from flytekit.sdk import types as _types
from flytekit.sdk import workflow as _workflow


def test_default_assumable_iam_role():
    with _configuration.TemporaryConfiguration(
        _os.path.join(
            _os.path.dirname(_os.path.realpath(__file__)),
            "../../common/configs/local.config",
        )
    ):
        workflow_to_test = _workflow.workflow(
            {},
            inputs={
                "required_input": _workflow.Input(_types.Types.Integer),
                "default_input": _workflow.Input(_types.Types.Integer, default=5),
            },
        )
        lp = workflow_to_test.create_launch_plan()
        assert lp.auth_role.assumable_iam_role == "arn:aws:iam::ABC123:role/my-flyte-role"


def test_hard_coded_assumable_iam_role():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(assumable_iam_role="override")
    assert lp.auth_role.assumable_iam_role == "override"


def test_default_deprecated_role():
    with _configuration.TemporaryConfiguration(
        _os.path.join(
            _os.path.dirname(_os.path.realpath(__file__)),
            "../../common/configs/deprecated_local.config",
        )
    ):
        workflow_to_test = _workflow.workflow(
            {},
            inputs={
                "required_input": _workflow.Input(_types.Types.Integer),
                "default_input": _workflow.Input(_types.Types.Integer, default=5),
            },
        )
        lp = workflow_to_test.create_launch_plan()
        assert lp.auth_role.assumable_iam_role == "arn:aws:iam::ABC123:role/my-flyte-role"


def test_hard_coded_deprecated_role():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(role="override")
    assert lp.auth_role.assumable_iam_role == "override"


def test_kubernetes_service_account():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(kubernetes_service_account="kube-service-acct")
    assert lp.auth_role.kubernetes_service_account == "kube-service-acct"


def test_fixed_inputs():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(fixed_inputs={"required_input": 4})
    assert len(lp.fixed_inputs.literals) == 1
    assert lp.fixed_inputs.literals["required_input"].scalar.primitive.integer == 4
    assert len(lp.default_inputs.parameters) == 1
    assert lp.default_inputs.parameters["default_input"].default.scalar.primitive.integer == 5


def test_redefining_inputs_good():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        default_inputs={"required_input": _workflow.Input(_types.Types.Integer, default=900)}
    )
    assert len(lp.fixed_inputs.literals) == 0
    assert len(lp.default_inputs.parameters) == 2
    assert lp.default_inputs.parameters["required_input"].default.scalar.primitive.integer == 900
    assert lp.default_inputs.parameters["default_input"].default.scalar.primitive.integer == 5


def test_no_additional_inputs():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan()
    assert len(lp.fixed_inputs.literals) == 0
    assert lp.default_inputs.parameters["default_input"].default.scalar.primitive.integer == 5
    assert lp.default_inputs.parameters["required_input"].required is True


@_pytest.mark.parametrize(
    "schedule,cron_expression,cron_schedule",
    [
        (_schedules.CronSchedule("* * ? * * *"), "* * ? * * *", None),
        (_schedules.CronSchedule(cron_expression="* * ? * * *"), "* * ? * * *", None),
        (_schedules.CronSchedule(cron_expression="0/15 * * * ? *"), "0/15 * * * ? *", None),
        (_schedules.CronSchedule(schedule="* * * * *"), None, _schedule.Schedule.CronSchedule("* * * * *", None)),
        (
            _schedules.CronSchedule(schedule="* * * * *", offset="P1D"),
            None,
            _schedule.Schedule.CronSchedule("* * * * *", "P1D"),
        ),
    ],
)
def test_schedule(schedule, cron_expression, cron_schedule):
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
        schedule=schedule,
        role="what",
    )
    assert lp.entity_metadata.schedule.kickoff_time_input_arg is None
    assert lp.entity_metadata.schedule.cron_expression == cron_expression
    assert lp.entity_metadata.schedule.cron_schedule == cron_schedule
    assert lp.is_scheduled


def test_no_schedule():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan()
    assert lp.entity_metadata.schedule.kickoff_time_input_arg == ""
    assert lp.entity_metadata.schedule.schedule_expression is None
    assert not lp.is_scheduled


def test_schedule_pointing_to_datetime():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Datetime),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        schedule=_schedules.CronSchedule("* * ? * * *", kickoff_time_input_arg="required_input"),
        role="what",
    )
    assert lp.entity_metadata.schedule.kickoff_time_input_arg == "required_input"
    assert lp.entity_metadata.schedule.cron_expression == "* * ? * * *"


def test_notifications():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        notifications=[_notifications.PagerDuty([_execution.WorkflowExecutionPhase.FAILED], ["me@myplace.com"])]
    )
    assert len(lp.entity_metadata.notifications) == 1
    assert lp.entity_metadata.notifications[0].pager_duty.recipients_email == ["me@myplace.com"]
    assert lp.entity_metadata.notifications[0].phases == [_execution.WorkflowExecutionPhase.FAILED]


def test_no_notifications():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan()
    assert len(lp.entity_metadata.notifications) == 0


def test_launch_plan_node():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
        outputs={"out": _workflow.Output([1, 2, 3], sdk_type=[_types.Types.Integer])},
    )
    lp = workflow_to_test.create_launch_plan()

    # Test that required input isn't set
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        lp()

    # Test that positional args are rejected
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        lp(1, 2)

    # Test that type checking works
    with _pytest.raises(_user_exceptions.FlyteTypeException):
        lp(required_input="abc", default_input=1)

    # Test that bad arg name is detected
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        lp(required_input=1, bad_arg=1)

    # Test default input is accounted for
    n = lp(required_input=10)
    assert n.inputs[0].var == "default_input"
    assert n.inputs[0].binding.scalar.primitive.integer == 5
    assert n.inputs[1].var == "required_input"
    assert n.inputs[1].binding.scalar.primitive.integer == 10

    # Test default input is overridden
    n = lp(required_input=10, default_input=50)
    assert n.inputs[0].var == "default_input"
    assert n.inputs[0].binding.scalar.primitive.integer == 50
    assert n.inputs[1].var == "required_input"
    assert n.inputs[1].binding.scalar.primitive.integer == 10

    # Test that launch plan ID ref is flexible
    lp._id = "fake"
    assert n.workflow_node.launchplan_ref == "fake"
    lp._id = None

    # Test that outputs are promised
    n.assign_id_and_return("node-id")
    assert n.outputs["out"].sdk_type.to_flyte_literal_type().collection_type.simple == _type_models.SimpleType.INTEGER
    assert n.outputs["out"].var == "out"
    assert n.outputs["out"].node_id == "node-id"


def test_labels():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
        schedule=_schedules.CronSchedule("* * ? * * *"),
        role="what",
        labels=_common_models.Labels({"my": "label"}),
    )
    assert lp.labels.values == {"my": "label"}


def test_annotations():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
        schedule=_schedules.CronSchedule("* * ? * * *"),
        role="what",
        annotations=_common_models.Annotations({"my": "annotation"}),
    )
    assert lp.annotations.values == {"my": "annotation"}


def test_serialize():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    workflow_to_test.id = _identifier.Identifier(_identifier.ResourceType.WORKFLOW, "p", "d", "n", "v")
    lp = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
        role="iam_role",
    )

    with _configuration.TemporaryConfiguration(
        _os.path.join(
            _os.path.dirname(_os.path.realpath(__file__)),
            "../../common/configs/local.config",
        ),
        internal_overrides={"image": "myflyteimage:v123", "project": "myflyteproject", "domain": "development"},
    ):
        s = lp.serialize()

    assert (
        s.spec.workflow_id
        == _identifier.Identifier(_identifier.ResourceType.WORKFLOW, "p", "d", "n", "v").to_flyte_idl()
    )
    assert s.spec.auth_role.assumable_iam_role == "iam_role"
    assert s.spec.default_inputs.parameters["default_input"].default.scalar.primitive.integer == 5


def test_promote_from_model():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    workflow_to_test.id = _identifier.Identifier(_identifier.ResourceType.WORKFLOW, "p", "d", "n", "v")
    lp = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
        schedule=_schedules.CronSchedule("* * ? * * *"),
        role="what",
        labels=_common_models.Labels({"my": "label"}),
    )

    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _launch_plan.SdkRunnableLaunchPlan.from_flyte_idl(lp.to_flyte_idl())

    lp_from_spec = _launch_plan.SdkLaunchPlan.from_flyte_idl(lp.to_flyte_idl())
    assert not isinstance(lp_from_spec, _launch_plan.SdkRunnableLaunchPlan)
    assert isinstance(lp_from_spec, _launch_plan.SdkLaunchPlan)
    assert lp_from_spec == lp


def test_raw_data_output_prefix():
    workflow_to_test = _workflow.workflow(
        {},
        inputs={
            "required_input": _workflow.Input(_types.Types.Integer),
            "default_input": _workflow.Input(_types.Types.Integer, default=5),
        },
    )
    lp = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
        raw_output_data_prefix="s3://bucket-name",
    )
    assert lp.raw_output_data_config.output_location_prefix == "s3://bucket-name"

    lp2 = workflow_to_test.create_launch_plan(
        fixed_inputs={"required_input": 5},
    )
    assert lp2.raw_output_data_config.output_location_prefix == ""

import datetime

import pytest

from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.schedule import CronSchedule, FixedRate
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.models import schedule as _schedule_models


def test_cron():
    with pytest.raises(AssertionError):
        obj = CronSchedule("* * ? * * *", kickoff_time_input_arg="abc")
        assert obj.kickoff_time_input_arg == "abc"
        assert obj.cron_expression == "* * ? * * *"
        assert obj == CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_cron_karg():
    with pytest.raises(AssertionError):
        obj = CronSchedule(cron_expression="* * ? * * *", kickoff_time_input_arg="abc")
        assert obj.kickoff_time_input_arg == "abc"
        assert obj.cron_expression == "* * ? * * *"
        assert obj == CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_cron_validation():
    with pytest.raises(AssertionError):
        CronSchedule("* * * * * *", kickoff_time_input_arg="abc")

    with pytest.raises(AssertionError):
        CronSchedule("* * ? * *", kickoff_time_input_arg="abc")


def test_fixed_rate():
    obj = FixedRate(datetime.timedelta(hours=10), kickoff_time_input_arg="abc")
    assert obj.rate.unit == FixedRate.FixedRateUnit.HOUR
    assert obj.rate.value == 10
    assert obj == FixedRate.from_flyte_idl(obj.to_flyte_idl())

    obj = FixedRate(datetime.timedelta(hours=24), kickoff_time_input_arg="abc")
    assert obj.rate.unit == FixedRate.FixedRateUnit.DAY
    assert obj.rate.value == 1
    assert obj == FixedRate.from_flyte_idl(obj.to_flyte_idl())

    obj = FixedRate(datetime.timedelta(minutes=30), kickoff_time_input_arg="abc")
    assert obj.rate.unit == FixedRate.FixedRateUnit.MINUTE
    assert obj.rate.value == 30
    assert obj == FixedRate.from_flyte_idl(obj.to_flyte_idl())

    obj = FixedRate(datetime.timedelta(minutes=120), kickoff_time_input_arg="abc")
    assert obj.rate.unit == FixedRate.FixedRateUnit.HOUR
    assert obj.rate.value == 2
    assert obj == FixedRate.from_flyte_idl(obj.to_flyte_idl())


def test_fixed_rate_bad_duration():
    pass


def test_fixed_rate_negative_duration():
    pass


@pytest.mark.parametrize(
    "schedule",
    [
        "hourly",
        "hours",
        "HOURS",
        "@hourly",
        "daily",
        "days",
        "DAYS",
        "@daily",
        "weekly",
        "weeks",
        "WEEKS",
        "@weekly",
        "monthly",
        "months",
        "MONTHS",
        "@monthly",
        "annually",
        "@annually",
        "yearly",
        "years",
        "YEARS",
        "@yearly",
        "* * * * *",
    ],
)
def test_cron_schedule_schedule_validation(schedule):
    obj = CronSchedule(schedule=schedule, kickoff_time_input_arg="abc")
    assert obj.cron_schedule.schedule == schedule


@pytest.mark.parametrize(
    "schedule",
    ["foo", "* *"],
)
def test_cron_schedule_schedule_validation_invalid(schedule):
    with pytest.raises(ValueError):
        CronSchedule(schedule=schedule, kickoff_time_input_arg="abc")


def test_cron_schedule_offset_validation_invalid():
    with pytest.raises(ValueError):
        CronSchedule(schedule="days", offset="foo", kickoff_time_input_arg="abc")


def test_cron_schedule():
    obj = CronSchedule(schedule="days", kickoff_time_input_arg="abc")
    assert obj.cron_schedule.schedule == "days"
    assert obj.cron_schedule.offset is None
    assert obj == CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_cron_schedule_offset():
    obj = CronSchedule(schedule="days", offset="P1D", kickoff_time_input_arg="abc")
    assert obj.cron_schedule.schedule == "days"
    assert obj.cron_schedule.offset == "P1D"
    assert obj == CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_both_cron_expression_and_cron_schedule_schedule():
    with pytest.raises(AssertionError):
        CronSchedule(cron_expression="* * ? * * *", schedule="days", offset="foo", kickoff_time_input_arg="abc")


def test_cron_expression_and_cron_schedule_offset():
    with pytest.raises(AssertionError):
        CronSchedule(cron_expression="* * ? * * *", offset="foo", kickoff_time_input_arg="abc")


def test_schedule_with_lp():
    @task
    def double(a: int) -> int:
        return a * 2

    @workflow
    def quadruple(a: int) -> int:
        b = double(a=a)
        c = double(a=b)
        return c

    lp = LaunchPlan.create(
        "schedule_test",
        quadruple,
        schedule=FixedRate(datetime.timedelta(hours=12), "kickoff_input"),
    )
    assert lp.schedule == _schedule_models.Schedule(
        "kickoff_input", rate=_schedule_models.Schedule.FixedRate(12, _schedule_models.Schedule.FixedRateUnit.HOUR)
    )


def test_cron_invalid_date_combinations():
    """Test that CronSchedule rejects invalid date combinations like 31st of February."""
    
    # Test invalid date combinations
    invalid_schedules = [
        "0 0 31 2 *",    # February 31st (does not exist)
        "0 0 30 2 *",    # February 30th (does not exist)
        "0 0 31 4 *",    # April 31st (does not exist)
        "0 0 31 6 *",    # June 31st (does not exist)
    ]
    
    for invalid_schedule in invalid_schedules:
        with pytest.raises(ValueError, match="Schedule contains invalid date combinations"):
            CronSchedule(schedule=invalid_schedule)
    
    # Test valid date combinations that should pass
    valid_schedules = [
        "0 0 28 2 *",    # February 28th (always valid)
        "0 0 29 2 *",    # February 29th (valid in leap years - handled by croniter)
        "0 0 30 4 *",    # April 30th (valid)
        "0 0 31 1 *",    # January 31st (valid)
        "0 0 31 3 *",    # March 31st (valid)
    ]
    
    for valid_schedule in valid_schedules:
        # These should not raise any exceptions
        obj = CronSchedule(schedule=valid_schedule)
        assert obj.cron_schedule.schedule == valid_schedule

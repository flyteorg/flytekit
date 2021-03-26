import datetime as _datetime

import pytest as _pytest

from flytekit.common import schedules as _schedules
from flytekit.common.exceptions import user as _user_exceptions


def test_cron():
    obj = _schedules.CronSchedule("* * ? * * *", kickoff_time_input_arg="abc")
    assert obj.kickoff_time_input_arg == "abc"
    assert obj.cron_expression == "* * ? * * *"
    assert obj == _schedules.CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_cron_karg():
    obj = _schedules.CronSchedule(cron_expression="* * ? * * *", kickoff_time_input_arg="abc")
    assert obj.kickoff_time_input_arg == "abc"
    assert obj.cron_expression == "* * ? * * *"
    assert obj == _schedules.CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_cron_validation():
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _schedules.CronSchedule("* * * * * *", kickoff_time_input_arg="abc")

    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _schedules.CronSchedule("* * ? * *", kickoff_time_input_arg="abc")


def test_fixed_rate():
    obj = _schedules.FixedRate(_datetime.timedelta(hours=10), kickoff_time_input_arg="abc")
    assert obj.rate.unit == _schedules.FixedRate.FixedRateUnit.HOUR
    assert obj.rate.value == 10
    assert obj == _schedules.FixedRate.from_flyte_idl(obj.to_flyte_idl())

    obj = _schedules.FixedRate(_datetime.timedelta(hours=24), kickoff_time_input_arg="abc")
    assert obj.rate.unit == _schedules.FixedRate.FixedRateUnit.DAY
    assert obj.rate.value == 1
    assert obj == _schedules.FixedRate.from_flyte_idl(obj.to_flyte_idl())

    obj = _schedules.FixedRate(_datetime.timedelta(minutes=30), kickoff_time_input_arg="abc")
    assert obj.rate.unit == _schedules.FixedRate.FixedRateUnit.MINUTE
    assert obj.rate.value == 30
    assert obj == _schedules.FixedRate.from_flyte_idl(obj.to_flyte_idl())

    obj = _schedules.FixedRate(_datetime.timedelta(minutes=120), kickoff_time_input_arg="abc")
    assert obj.rate.unit == _schedules.FixedRate.FixedRateUnit.HOUR
    assert obj.rate.value == 2
    assert obj == _schedules.FixedRate.from_flyte_idl(obj.to_flyte_idl())


def test_fixed_rate_bad_duration():
    pass


def test_fixed_rate_negative_duration():
    pass


@_pytest.mark.parametrize(
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
    obj = _schedules.CronSchedule(schedule=schedule, kickoff_time_input_arg="abc")
    assert obj.cron_schedule.schedule == schedule


@_pytest.mark.parametrize(
    "schedule",
    ["foo", "* *"],
)
def test_cron_schedule_schedule_validation_invalid(schedule):
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _schedules.CronSchedule(schedule=schedule, kickoff_time_input_arg="abc")


def test_cron_schedule_offset_validation_invalid():
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _schedules.CronSchedule(schedule="days", offset="foo", kickoff_time_input_arg="abc")


def test_cron_schedule():
    obj = _schedules.CronSchedule(schedule="days", kickoff_time_input_arg="abc")
    assert obj.cron_schedule.schedule == "days"
    assert obj.cron_schedule.offset is None
    assert obj == _schedules.CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_cron_schedule_offset():
    obj = _schedules.CronSchedule(schedule="days", offset="P1D", kickoff_time_input_arg="abc")
    assert obj.cron_schedule.schedule == "days"
    assert obj.cron_schedule.offset == "P1D"
    assert obj == _schedules.CronSchedule.from_flyte_idl(obj.to_flyte_idl())


def test_both_cron_expression_and_cron_schedule_schedule():
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _schedules.CronSchedule(
            cron_expression="* * ? * * *", schedule="days", offset="foo", kickoff_time_input_arg="abc"
        )


def test_cron_expression_and_cron_schedule_offset():
    with _pytest.raises(_user_exceptions.FlyteAssertion):
        _schedules.CronSchedule(cron_expression="* * ? * * *", offset="foo", kickoff_time_input_arg="abc")

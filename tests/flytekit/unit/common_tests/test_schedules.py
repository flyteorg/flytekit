import datetime as _datetime

import pytest as _pytest

from flytekit.common import schedules as _schedules
from flytekit.common.exceptions import user as _user_exceptions


def test_cron():
    obj = _schedules.CronSchedule("* * ? * * *", kickoff_time_input_arg="abc")
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


def test_cron_schedule_with_offset():
    obj = _schedules.CronScheduleWithOffset("days", kickoff_time_input_arg="abc")
    assert obj.cron_schedule_with_offset.schedule == "days"
    assert obj.cron_schedule_with_offset.offset is None
    assert obj == _schedules.CronScheduleWithOffset.from_flyte_idl(obj.to_flyte_idl())

    obj = _schedules.CronScheduleWithOffset("days", "P1D", kickoff_time_input_arg="abc")
    assert obj.cron_schedule_with_offset.schedule == "days"
    assert obj.cron_schedule_with_offset.offset == "P1D"
    assert obj == _schedules.CronScheduleWithOffset.from_flyte_idl(obj.to_flyte_idl())

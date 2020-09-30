from flytekit.models import schedule as _schedule


def test_schedule():
    obj = _schedule.Schedule(kickoff_time_input_arg="fdsa", cron_expression="1 2 3 4 5 6")
    assert obj.rate is None
    assert obj.cron_expression == "1 2 3 4 5 6"
    assert obj.schedule_expression == "1 2 3 4 5 6"
    assert obj.kickoff_time_input_arg == "fdsa"

    obj2 = _schedule.Schedule.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.rate is None
    assert obj2.cron_expression == "1 2 3 4 5 6"
    assert obj2.schedule_expression == "1 2 3 4 5 6"
    assert obj2.kickoff_time_input_arg == "fdsa"


def test_schedule_fixed_rate():
    fr = _schedule.Schedule.FixedRate(10, _schedule.Schedule.FixedRateUnit.MINUTE)
    obj = _schedule.Schedule(kickoff_time_input_arg="fdsa", rate=fr)
    assert obj.rate is not None
    assert obj.cron_expression is None
    assert obj.kickoff_time_input_arg == "fdsa"
    assert obj.rate == fr
    assert obj.schedule_expression == fr

    obj2 = _schedule.Schedule.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.rate is not None
    assert obj2.cron_expression is None
    assert obj2.kickoff_time_input_arg == "fdsa"
    assert obj2.rate == fr
    assert obj2.schedule_expression == fr


def test_schedule_with_offset():
    cso = _schedule.Schedule.CronScheduleWithOffset("days", None)
    obj = _schedule.Schedule(cron_schedule_with_offset=cso, kickoff_time_input_arg="fdsa")
    assert obj.cron_schedule_with_offset.schedule == "days"
    assert obj.cron_schedule_with_offset.offset is None
    assert obj.rate is None
    assert obj.cron_expression is None

    obj2 = _schedule.Schedule.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.cron_schedule_with_offset.schedule == "days"
    assert obj2.cron_schedule_with_offset.offset == ""
    assert obj.rate is None
    assert obj.cron_expression is None

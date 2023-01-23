from datetime import datetime

from flytekit import CronSchedule, LaunchPlan, task, workflow


@task
def tk(t: datetime, v: int):
    print(f"Invoked at {t} with v {v}")


@workflow
def example_wf(t: datetime, v: int):
    tk(t=t, v=v)


daily_lp = LaunchPlan.get_or_create(
    workflow=example_wf,
    name="daily",
    fixed_inputs={"v": 10},
    schedule=CronSchedule(schedule="0 8 * * *", kickoff_time_input_arg="t"),
)

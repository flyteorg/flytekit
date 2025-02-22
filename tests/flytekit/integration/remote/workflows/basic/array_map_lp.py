import typing
from flytekit import map_task, task, workflow, LaunchPlan


@task
def fn(x: int) -> int:
    return x + 1


@workflow
def test_map_over_lp_wf(x: int) -> int:
    return fn(x=x)

lp = LaunchPlan.get_or_create(
    workflow=test_map_over_lp_wf,
    name="test_map_over_lp_wf_lp",
)

@workflow
def workflow_with_map_over_lp(data: typing.List[int]) -> typing.List[int]:
    return map_task(lp)(x=data)
import typing

import pytest

from flytekit.core import launch_plan
from flytekit.core.task import task
from flytekit.core.workflow import workflow


def test_lp():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @task
    def t2(a: str, b: str) -> str:
        return b + a

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

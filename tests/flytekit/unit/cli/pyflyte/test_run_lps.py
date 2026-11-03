import click

from flytekit import task, workflow, LaunchPlan
from flytekit.clis.sdk_in_container.run import WorkflowCommand, RunLevelParams

import mock
import pytest


@task
def two_inputs(x: int, y: str) -> str:
    return f"{x},{y}"

@workflow
def two_inputs_wf(x: int, y: str) -> str:
    return two_inputs(x, y)

lp_fixed_y_default_x = LaunchPlan.get_or_create(
    workflow=two_inputs_wf,
    name="fixed-default-inputs",
    fixed_inputs={"y": "hello"},
    default_inputs={"x": 1}
)

lp_fixed_y = LaunchPlan.get_or_create(
    workflow=two_inputs_wf,
    name="fixed-y",
    fixed_inputs={"y": "hello"},
)

lp_fixed_x = LaunchPlan.get_or_create(
    workflow=two_inputs_wf,
    name="fixed-x",
    fixed_inputs={"x": 1},
)

lp_fixed_all = LaunchPlan.get_or_create(
    workflow=two_inputs_wf,
    name="fixed-all",
    fixed_inputs={"x": 1, "y": "test"},
)

lp_default_x = LaunchPlan.get_or_create(
    name="default-inputs",
    workflow=two_inputs_wf,
    default_inputs={"x": 1}
)

lp_simple = LaunchPlan.get_or_create(
    workflow=two_inputs_wf,
    name="no-fixed-default",
)

@pytest.mark.parametrize("lp_execs", [
    (lp_fixed_y_default_x, {"x": 1}),
    (lp_fixed_y, {"x": None}),
    (lp_fixed_x, {"y": None}),
    (lp_fixed_all, {}),
    (lp_default_x, {"y": None, "x": 1}),
    (lp_simple, {"x": None, "y": None}),
])
def test_workflowcommand_create_command(lp_execs):
    cmd = WorkflowCommand("testfile.py")
    rp =  RunLevelParams()
    ctx = click.Context(cmd, obj=rp)
    lp, exp_opts = lp_execs
    opts = cmd._create_command(ctx, "test_entity", rp, lp, "launch plan").params
    for o in opts:
        if "input" in o.name:
            continue
        assert o.name in exp_opts
        assert o.default == exp_opts[o.name]

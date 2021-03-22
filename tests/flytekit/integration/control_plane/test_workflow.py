import datetime
import os
import pathlib
import time

import pytest

from flytekit.common.exceptions.user import FlyteAssertion
from flytekit.control_plane import launch_plan
from flytekit.models import literals

PROJECT = "flytesnacks"
VERSION = os.getpid()


@pytest.fixture(scope="session")
def flyte_workflows_source_dir():
    return pathlib.Path(os.path.dirname(__file__)) / "mock_flyte_repo"


@pytest.fixture(scope="session")
def flyte_workflows_register(docker_compose):
    docker_compose.execute(
        f"exec -w /flyteorg/src -e SANDBOX=1 -e PROJECT={PROJECT} -e VERSION=v{VERSION} "
        "backend make -C workflows register"
    )


def test_client(flyteclient):
    projects = flyteclient.list_projects_paginated(limit=5, token=None)
    assert len(projects) <= 5


def test_launch_workflow(flyteclient, flyte_workflows_register):
    execution = launch_plan.FlyteLaunchPlan.fetch(
        PROJECT, "development", "workflows.basic.hello_world.my_wf", f"v{VERSION}"
    ).launch_with_literals(PROJECT, "development", literals.LiteralMap({}))
    execution.wait_for_completion()
    assert execution.outputs.literals["o0"].scalar.primitive.string_value == "hello world"


def test_launch_workflow_with_args(flyteclient, flyte_workflows_register):
    execution = launch_plan.FlyteLaunchPlan.fetch(
        PROJECT, "development", "workflows.basic.basic_workflow.my_wf", f"v{VERSION}"
    ).launch_with_literals(
        PROJECT,
        "development",
        literals.LiteralMap(
            {
                "a": literals.Literal(literals.Scalar(literals.Primitive(integer=10))),
                "b": literals.Literal(literals.Scalar(literals.Primitive(string_value="foobar"))),
            }
        ),
    )
    execution.wait_for_completion()
    assert execution.outputs.literals["o0"].scalar.primitive.integer == 12
    assert execution.outputs.literals["o1"].scalar.primitive.string_value == "foobarworld"


def test_monitor_workflow(flyteclient, flyte_workflows_register):
    execution = launch_plan.FlyteLaunchPlan.fetch(
        PROJECT, "development", "workflows.basic.hello_world.my_wf", f"v{VERSION}"
    ).launch_with_literals(PROJECT, "development", literals.LiteralMap({}))

    poll_interval = datetime.timedelta(seconds=1)
    time_to_give_up = datetime.datetime.utcnow() + datetime.timedelta(seconds=60)

    execution.sync()
    while datetime.datetime.utcnow() < time_to_give_up:

        if execution.is_complete:
            execution.sync()
            break

        with pytest.raises(
            FlyteAssertion, match="Please wait until the node execution has completed before requesting the outputs"
        ):
            execution.outputs

        time.sleep(poll_interval.total_seconds())
        execution.sync()

        if execution.node_executions:
            assert execution.node_executions["start-node"].closure.phase == 3  # SUCCEEEDED

    for key in execution.node_executions:
        assert execution.node_executions[key].closure.phase == 3

    assert execution.node_executions["n0"].outputs.literals["o0"].scalar.primitive.string_value == "hello world"
    assert execution.outputs.literals["o0"].scalar.primitive.string_value == "hello world"

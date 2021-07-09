import datetime
import os
import pathlib
import time

import pytest

from flytekit.common.exceptions.user import FlyteAssertion, FlyteEntityAlreadyExistsException
from flytekit.core.launch_plan import LaunchPlan
from flytekit.remote.remote import FlyteRemote

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


def test_client(flyteclient, flyte_workflows_register):
    projects = flyteclient.list_projects_paginated(limit=5, token=None)
    assert len(projects) <= 5


def test_fetch_execute_launch_plan(flyteclient, flyte_workflows_register):
    remote = FlyteRemote()
    flyte_launch_plan = remote.fetch_launch_plan(
        PROJECT, "development", "workflows.basic.hello_world.my_wf", f"v{VERSION}"
    )
    execution = remote.execute(flyte_launch_plan, {})
    execution.wait_for_completion()
    assert execution.outputs["o0"] == "hello world"


def fetch_execute_launch_plan_with_args(flyteclient, flyte_workflows_register):
    remote = FlyteRemote()
    flyte_launch_plan = remote.fetch_launch_plan(
        PROJECT, "development", "workflows.basic.basic_workflow.my_wf", f"v{VERSION}"
    )
    execution = remote.execute(flyte_launch_plan, {"a": 10, "b": "foobar"})
    execution.wait_for_completion()
    assert execution.node_executions["n0"].inputs == {"a": 10}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 12, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": "world", "b": "foobar"}
    assert execution.node_executions["n1"].outputs == {"o0": "foobarworld"}
    assert execution.node_executions["n0"].task_executions[0].inputs == {"a": 10}
    assert execution.node_executions["n0"].task_executions[0].outputs == {"t1_int_output": 12, "c": "world"}
    assert execution.node_executions["n1"].task_executions[0].inputs == {"a": "world", "b": "foobar"}
    assert execution.node_executions["n1"].task_executions[0].outputs == {"o0": "foobarworld"}
    assert execution.inputs["a"] == 10
    assert execution.inputs["b"] == "foobar"
    assert execution.outputs["o0"] == 12
    assert execution.outputs["o1"] == "foobarworld"


def test_monitor_workflow_execution(flyteclient, flyte_workflows_register):
    remote = FlyteRemote()
    flyte_launch_plan = remote.fetch_launch_plan(
        PROJECT, "development", "workflows.basic.hello_world.my_wf", f"v{VERSION}"
    )
    execution = remote.execute(flyte_launch_plan, {})

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

    assert execution.node_executions["n0"].inputs == {}
    assert execution.node_executions["n0"].outputs["o0"] == "hello world"
    assert execution.node_executions["n0"].task_executions[0].inputs == {}
    assert execution.node_executions["n0"].task_executions[0].outputs["o0"] == "hello world"
    assert execution.inputs == {}
    assert execution.outputs["o0"] == "hello world"


def test_fetch_execute_launch_plan_with_subworkflows(flyteclient, flyte_workflows_register):
    remote = FlyteRemote()
    flyte_launch_plan = remote.fetch_launch_plan(
        PROJECT, "development", "workflows.basic.subworkflows.parent_wf", f"v{VERSION}"
    )
    execution = remote.execute(flyte_launch_plan, {"a": 101})
    execution.wait_for_completion()
    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 101}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 103, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": 103}
    assert execution.node_executions["n1"].outputs == {"o0": "world", "o1": "world"}

    # check subworkflow task execution inputs and outputs
    subworkflow_node_executions = execution.node_executions["n1"].subworkflow_node_executions
    subworkflow_node_executions["n1-0-n0"].inputs == {"a": 103}
    subworkflow_node_executions["n1-0-n1"].outputs == {"t1_int_output": 107, "c": "world"}


def test_fetch_execute_workflow(flyteclient, flyte_workflows_register):
    remote = FlyteRemote()
    flyte_workflow = remote.fetch_workflow(PROJECT, "development", "workflows.basic.hello_world.my_wf", f"v{VERSION}")
    execution = remote.execute(flyte_workflow, {})
    execution.wait_for_completion()
    assert execution.outputs["o0"] == "hello world"


def test_fetch_execute_task(flyteclient, flyte_workflows_register):
    remote = FlyteRemote()
    flyte_task = remote.fetch_task(PROJECT, "development", "workflows.basic.basic_workflow.t1", f"v{VERSION}")
    execution = remote.execute(flyte_task, {"a": 10})
    execution.wait_for_completion()
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"


def _set_env():
    os.environ["FLYTE_INTERNAL_PROJECT"] = PROJECT
    os.environ["FLYTE_INTERNAL_DOMAIN"] = "development"
    os.environ["FLYTE_INTERNAL_VERSION"] = f"v{VERSION}"
    os.environ["FLYTE_INTERNAL_IMAGE"] = "default:tag"


def test_execute_python_task(flyteclient, flyte_workflows_register):
    """Test execution of a @task-decorated python function that is already registered."""
    from mock_flyte_repo.workflows.basic.basic_workflow import t1

    # make sure the task name is the same as the name used during registration
    t1._name = t1.name.replace("mock_flyte_repo.", "")
    _set_env()

    remote = FlyteRemote()
    execution = remote.execute(t1, inputs={"a": 10})
    execution.wait_for_completion()
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"


def test_execute_python_workflow_and_launch_plan(flyteclient, flyte_workflows_register):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from mock_flyte_repo.workflows.basic.basic_workflow import my_wf

    # make sure the task name is the same as the name used during registration
    my_wf._name = my_wf.name.replace("mock_flyte_repo.", "")
    _set_env()

    remote = FlyteRemote()
    execution = remote.execute(my_wf, inputs={"a": 10, "b": "xyz"})
    execution.wait_for_completion()
    assert execution.outputs["o0"] == 12
    assert execution.outputs["o1"] == "xyzworld"

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(launch_plan, inputs={"a": 14, "b": "foobar"})
    execution.wait_for_completion()
    assert execution.outputs["o0"] == 16
    assert execution.outputs["o1"] == "foobarworld"


def test_register(flyteclient, flyte_workflows_register):
    _set_env()

    remote = FlyteRemote()
    flyte_task = remote.fetch_task(PROJECT, "development", "workflows.basic.basic_workflow.t1", f"v{VERSION}")

    with pytest.raises(FlyteEntityAlreadyExistsException):
        remote.register(flyte_task)

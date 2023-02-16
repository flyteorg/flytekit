import datetime
import json
import os
import pathlib
import time
import typing

import joblib
import pytest

from flytekit import LaunchPlan, kwtypes
from flytekit.configuration import Config
from flytekit.exceptions.user import FlyteAssertion, FlyteEntityNotExistException
from flytekit.extras.sqlite3.task import SQLite3Config, SQLite3Task
from flytekit.remote.remote import FlyteRemote
from flytekit.types.schema import FlyteSchema

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


@pytest.fixture(scope="session")
def flyte_remote_env(docker_services):
    os.environ["FLYTE_INTERNAL_PROJECT"] = PROJECT
    os.environ["FLYTE_INTERNAL_DOMAIN"] = "development"
    os.environ["FLYTE_INTERNAL_VERSION"] = f"v{VERSION}"
    os.environ["FLYTE_INTERNAL_IMAGE"] = "default:tag"
    os.environ["FLYTE_CLOUD_PROVIDER"] = "aws"
    os.environ["FLYTE_AWS_ENDPOINT"] = f"http://localhost:{docker_services.port_for('backend', 30084)}"
    os.environ["FLYTE_AWS_ACCESS_KEY_ID"] = "minio"
    os.environ["FLYTE_AWS_SECRET_ACCESS_KEY"] = "miniostorage"


def test_client(flyteclient, flyte_workflows_register, docker_services):
    projects = flyteclient.list_projects_paginated(limit=5, token=None)
    assert len(projects) <= 5


def test_fetch_execute_launch_plan(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_launch_plan = remote.fetch_launch_plan(name="workflows.basic.hello_world.my_wf", version=f"v{VERSION}")
    execution = remote.execute(flyte_launch_plan, {}, wait=True)
    assert execution.outputs["o0"] == "hello world"


def fetch_execute_launch_plan_with_args(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_launch_plan = remote.fetch_launch_plan(name="workflows.basic.basic_workflow.my_wf", version=f"v{VERSION}")
    execution = remote.execute(flyte_launch_plan, {"a": 10, "b": "foobar"}, wait=True)
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


def test_monitor_workflow_execution(flyteclient, flyte_workflows_register, flyte_remote_env):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_launch_plan = remote.fetch_launch_plan(name="workflows.basic.hello_world.my_wf", version=f"v{VERSION}")
    execution = remote.execute(flyte_launch_plan, {})

    poll_interval = datetime.timedelta(seconds=1)
    time_to_give_up = datetime.datetime.utcnow() + datetime.timedelta(seconds=60)

    execution = remote.sync_execution(execution, sync_nodes=True)
    while datetime.datetime.utcnow() < time_to_give_up:

        if execution.is_done:
            break

        with pytest.raises(
            FlyteAssertion, match="Please wait until the node execution has completed before requesting the outputs"
        ):
            execution.outputs

        time.sleep(poll_interval.total_seconds())
        execution = remote.sync_execution(execution, sync_nodes=True)

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
    remote = FlyteRemote(Config.auto(), PROJECT, "development")

    flyte_launch_plan = remote.fetch_launch_plan(name="workflows.basic.subworkflows.parent_wf", version=f"v{VERSION}")
    execution = remote.execute(flyte_launch_plan, {"a": 101}, wait=True)
    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 101}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 103, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": 103}
    assert execution.node_executions["n1"].outputs == {"o0": "world", "o1": "world"}

    # check subworkflow task execution inputs and outputs
    subworkflow_node_executions = execution.node_executions["n1"].subworkflow_node_executions
    subworkflow_node_executions["n1-0-n0"].inputs == {"a": 103}
    subworkflow_node_executions["n1-0-n1"].outputs == {"t1_int_output": 107, "c": "world"}


def test_fetch_execute_launch_plan_with_child_workflows(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")

    flyte_launch_plan = remote.fetch_launch_plan(name="workflows.basic.child_workflow.parent_wf", version=f"v{VERSION}")
    execution = remote.execute(flyte_launch_plan, {"a": 3}, wait=True)

    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 3}
    assert execution.node_executions["n0"].outputs["o0"] == 6
    assert execution.node_executions["n1"].inputs == {"a": 6}
    assert execution.node_executions["n1"].outputs["o0"] == 12
    assert execution.node_executions["n2"].inputs == {"a": 6, "b": 12}
    assert execution.node_executions["n2"].outputs["o0"] == 18


def test_fetch_execute_workflow(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_workflow = remote.fetch_workflow(name="workflows.basic.hello_world.my_wf", version=f"v{VERSION}")
    execution = remote.execute(flyte_workflow, {}, wait=True)
    assert execution.outputs["o0"] == "hello world"
    assert isinstance(execution.closure.duration, datetime.timedelta)
    assert execution.closure.duration > datetime.timedelta(seconds=1)

    execution_to_terminate = remote.execute(flyte_workflow, {})
    remote.terminate(execution_to_terminate, cause="just because")


def test_fetch_execute_task(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_task = remote.fetch_task(name="workflows.basic.basic_workflow.t1", version=f"v{VERSION}")
    execution = remote.execute(flyte_task, {"a": 10}, wait=True)
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"
    assert execution.raw_inputs.get("a", int) == 10
    assert execution.raw_outputs.get("c", str) == "world"


def test_execute_python_task(flyteclient, flyte_workflows_register, flyte_remote_env):
    """Test execution of a @task-decorated python function that is already registered."""
    from mock_flyte_repo.workflows.basic.basic_workflow import t1

    # make sure the task name is the same as the name used during registration
    t1._name = t1.name.replace("mock_flyte_repo.", "")

    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    execution = remote.execute(t1, inputs={"a": 10}, version=f"v{VERSION}", wait=True)
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"


def test_execute_python_workflow_and_launch_plan(flyteclient, flyte_workflows_register, flyte_remote_env):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from mock_flyte_repo.workflows.basic.basic_workflow import my_wf

    # make sure the task name is the same as the name used during registration
    my_wf._name = my_wf.name.replace("mock_flyte_repo.", "")

    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    execution = remote.execute(my_wf, inputs={"a": 10, "b": "xyz"}, version=f"v{VERSION}", wait=True)
    assert execution.outputs["o0"] == 12
    assert execution.outputs["o1"] == "xyzworld"

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(launch_plan, inputs={"a": 14, "b": "foobar"}, version=f"v{VERSION}", wait=True)
    assert execution.outputs["o0"] == 16
    assert execution.outputs["o1"] == "foobarworld"

    flyte_workflow_execution = remote.fetch_execution(name=execution.id.name)
    assert execution.inputs == flyte_workflow_execution.inputs
    assert execution.outputs == flyte_workflow_execution.outputs


def test_fetch_execute_launch_plan_list_of_floats(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_launch_plan = remote.fetch_launch_plan(name="workflows.basic.list_float_wf.my_wf", version=f"v{VERSION}")
    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    execution = remote.execute(flyte_launch_plan, inputs={"xs": xs}, wait=True)
    assert execution.outputs["o0"] == "[42.24, 999.1, 0.0001]"


def test_fetch_execute_task_list_of_floats(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_task = remote.fetch_task(name="workflows.basic.list_float_wf.concat_list", version=f"v{VERSION}")
    xs: typing.List[float] = [0.1, 0.2, 0.3, 0.4, -99999.7]
    execution = remote.execute(flyte_task, {"xs": xs}, wait=True)
    assert execution.outputs["o0"] == "[0.1, 0.2, 0.3, 0.4, -99999.7]"


def test_fetch_execute_task_convert_dict(flyteclient, flyte_workflows_register):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_task = remote.fetch_task(name="workflows.basic.dict_str_wf.convert_to_string", version=f"v{VERSION}")
    d: typing.Dict[str, str] = {"key1": "value1", "key2": "value2"}
    execution = remote.execute(flyte_task, {"d": d}, wait=True)
    remote.sync_execution(execution, sync_nodes=True)
    assert json.loads(execution.outputs["o0"]) == {"key1": "value1", "key2": "value2"}


def test_execute_python_workflow_dict_of_string_to_string(flyteclient, flyte_workflows_register, flyte_remote_env):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from mock_flyte_repo.workflows.basic.dict_str_wf import my_wf

    # make sure the task name is the same as the name used during registration
    my_wf._name = my_wf.name.replace("mock_flyte_repo.", "")

    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    d: typing.Dict[str, str] = {"k1": "v1", "k2": "v2"}
    execution = remote.execute(my_wf, inputs={"d": d}, version=f"v{VERSION}", wait=True)
    assert json.loads(execution.outputs["o0"]) == {"k1": "v1", "k2": "v2"}

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(
        launch_plan, inputs={"d": {"k2": "vvvv", "abc": "def"}}, version=f"v{VERSION}", wait=True
    )
    assert json.loads(execution.outputs["o0"]) == {"k2": "vvvv", "abc": "def"}


def test_execute_python_workflow_list_of_floats(flyteclient, flyte_workflows_register, flyte_remote_env):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from mock_flyte_repo.workflows.basic.list_float_wf import my_wf

    # make sure the task name is the same as the name used during registration
    my_wf._name = my_wf.name.replace("mock_flyte_repo.", "")
    remote = FlyteRemote(Config.auto(), PROJECT, "development")

    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    execution = remote.execute(my_wf, inputs={"xs": xs}, version=f"v{VERSION}", wait=True)
    assert execution.outputs["o0"] == "[42.24, 999.1, 0.0001]"

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(launch_plan, inputs={"xs": [-1.1, 0.12345]}, version=f"v{VERSION}", wait=True)
    assert execution.outputs["o0"] == "[-1.1, 0.12345]"


def test_execute_sqlite3_task(flyteclient, flyte_workflows_register, flyte_remote_env):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")

    example_db = "https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"
    interactive_sql_task = SQLite3Task(
        "basic_querying",
        query_template="select TrackId, Name from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLite3Config(
            uri=example_db,
            compressed=True,
        ),
    )
    registered_sql_task = remote.register(interactive_sql_task)
    execution = remote.execute(registered_sql_task, inputs={"limit": 10}, wait=True)
    output = execution.outputs["results"]
    result = output.open().all()
    assert result.__class__.__name__ == "DataFrame"
    assert "TrackId" in result
    assert "Name" in result


def test_execute_joblib_workflow(flyteclient, flyte_workflows_register, flyte_remote_env):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    flyte_workflow = remote.fetch_workflow(name="workflows.basic.joblib.joblib_workflow", version=f"v{VERSION}")
    input_obj = [1, 2, 3]
    execution = remote.execute(flyte_workflow, {"obj": input_obj}, wait=True)
    joblib_output = execution.outputs["o0"]
    joblib_output.download()
    output_obj = joblib.load(joblib_output.path)
    assert execution.outputs["o0"].extension() == "joblib"
    assert output_obj == input_obj


def test_execute_with_default_launch_plan(flyteclient, flyte_workflows_register, flyte_remote_env):
    from mock_flyte_repo.workflows.basic.subworkflows import parent_wf

    # make sure the task name is the same as the name used during registration
    parent_wf._name = parent_wf.name.replace("mock_flyte_repo.", "")

    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    execution = remote.execute(parent_wf, {"a": 101}, version=f"v{VERSION}", wait=True)
    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 101}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 103, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": 103}
    assert execution.node_executions["n1"].outputs == {"o0": "world", "o1": "world"}

    # check subworkflow task execution inputs and outputs
    subworkflow_node_executions = execution.node_executions["n1"].subworkflow_node_executions
    subworkflow_node_executions["n1-0-n0"].inputs == {"a": 103}
    subworkflow_node_executions["n1-0-n1"].outputs == {"t1_int_output": 107, "c": "world"}


def test_fetch_not_exist_launch_plan(flyteclient):
    remote = FlyteRemote(Config.auto(), PROJECT, "development")
    with pytest.raises(FlyteEntityNotExistException):
        remote.fetch_launch_plan(name="workflows.basic.list_float_wf.fake_wf", version=f"v{VERSION}")

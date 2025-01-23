import botocore.session
from contextlib import ExitStack, contextmanager
import datetime
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import time
import typing
import re
import joblib
from urllib.parse import urlparse
import uuid
import pytest
from unittest import mock

from flytekit import LaunchPlan, kwtypes, WorkflowExecutionPhase
from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.core.launch_plan import reference_launch_plan
from flytekit.core.task import reference_task
from flytekit.core.workflow import reference_workflow
from flytekit.exceptions.user import FlyteAssertion, FlyteEntityNotExistException
from flytekit.extras.sqlite3.task import SQLite3Config, SQLite3Task
from flytekit.remote.remote import FlyteRemote
from flyteidl.service import dataproxy_pb2 as _data_proxy_pb2
from flytekit.types.schema import FlyteSchema
from flytekit.clients.friendly import SynchronousFlyteClient as _SynchronousFlyteClient
from flytekit.configuration import PlatformConfig

from tests.flytekit.integration.remote.utils import SimpleFileTransfer


MODULE_PATH = pathlib.Path(__file__).parent / "workflows/basic"
CONFIG = os.environ.get("FLYTECTL_CONFIG", str(pathlib.Path.home() / ".flyte" / "config-sandbox.yaml"))
# Run `make build-dev` to build and push the image to the local registry.
IMAGE = os.environ.get("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")
PROJECT = "flytesnacks"
DOMAIN = "development"
VERSION = f"v{os.getpid()}"
DEST_DIR = "/tmp"


@pytest.fixture(scope="session")
def register():
    out = subprocess.run(
        [
            "pyflyte",
            "--verbose",
            "-c",
            CONFIG,
            "register",
            "--image",
            IMAGE,
            "--project",
            PROJECT,
            "--domain",
            DOMAIN,
            "--version",
            VERSION,
            MODULE_PATH,
        ]
    )
    assert out.returncode == 0


def run(file_name, wf_name, *args) -> str:
    # Copy the environment and set the environment variable
    out = subprocess.run(
        [
            "pyflyte",
            "--verbose",
            "-c",
            CONFIG,
            "run",
            "--remote",
            "--destination-dir",
            DEST_DIR,
            "--image",
            IMAGE,
            "--project",
            PROJECT,
            "--domain",
            DOMAIN,
            MODULE_PATH / file_name,
            wf_name,
            *args,
        ],
        capture_output=True,  # Capture the output streams
        text=True,  # Return outputs as strings (not bytes)
    )
    assert out.returncode == 0, (f"Command failed with return code {out.returncode}.\n"
                                 f"Standard Output: {out.stdout}\n"
                                 f"Standard Error: {out.stderr}\n")

    match = re.search(r'executions/([a-zA-Z0-9]+)', out.stdout)
    if match:
        execution_id = match.group(1)
        return execution_id

    return "Unknown"


def test_remote_run():
    # child_workflow.parent_wf asynchronously register a parent wf1 with child lp from another wf2.
    run("child_workflow.py", "parent_wf", "--a", "3")

    # run twice to make sure it will register a new version of the workflow.
    run("default_lp.py", "my_wf")


def test_remote_eager_run():
    # child_workflow.parent_wf asynchronously register a parent wf1 with child lp from another wf2.
    run("eager_example.py", "simple_eager_workflow", "--x", "3")


def test_pydantic_default_input_with_map_task():
    execution_id = run("pydantic_wf.py", "wf")
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    print("Execution Error:", execution.error)
    assert execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED, f"Execution failed with phase: {execution.closure.phase}"


def test_pydantic_default_input_with_map_task():
    execution_id = run("pydantic_wf.py", "wf")
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    print("Execution Error:", execution.error)
    assert execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED, f"Execution failed with phase: {execution.closure.phase}"


def test_generic_idl_flytetypes():
    os.environ["FLYTE_USE_OLD_DC_FORMAT"] = "true"
    # default inputs for flyte types in dataclass
    execution_id = run("generic_idl_flytetypes.py", "wf")
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    print("Execution Error:", execution.error)
    assert execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED, f"Execution failed with phase: {execution.closure.phase}"
    os.environ["FLYTE_USE_OLD_DC_FORMAT"] = "false"


def test_msgpack_idl_flytetypes():
    # default inputs for flyte types in dataclass
    execution_id = run("msgpack_idl_flytetypes.py", "wf")
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    print("Execution Error:", execution.error)
    assert execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED, f"Execution failed with phase: {execution.closure.phase}"


def test_fetch_execute_launch_plan(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_launch_plan = remote.fetch_launch_plan(name="basic.hello_world.my_wf", version=VERSION)
    execution = remote.execute(flyte_launch_plan, inputs={}, wait=True)
    assert execution.outputs["o0"] == "hello world"


def test_get_download_artifact_signed_url(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_launch_plan = remote.fetch_launch_plan(name="basic.basic_workflow.my_wf", version=VERSION)
    execution = remote.execute(flyte_launch_plan, inputs={"a": 10, "b": "foobar"}, wait=True)
    project, domain, name = execution.id.project, execution.id.domain, execution.id.name

    # Fetch the download deck signed URL for the execution
    client = _SynchronousFlyteClient(PlatformConfig.for_endpoint("localhost:30080", True))
    download_link_response = client.get_download_artifact_signed_url(
        node_id="n0",  # Assuming node_id is "n0"
        project=project,
        domain=domain,
        name=name,
        artifact_type=_data_proxy_pb2.ARTIFACT_TYPE_DECK,
    )

    # Check if the signed URL is valid and starts with the expected prefix
    signed_url = download_link_response.signed_url[0]
    assert signed_url.startswith(
        f"http://localhost:30002/my-s3-bucket/metadata/propeller/{project}-{domain}-{name}/n0/data/0/deck.html")


def test_fetch_execute_launch_plan_with_args(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_launch_plan = remote.fetch_launch_plan(name="basic.basic_workflow.my_wf", version=VERSION)
    execution = remote.execute(flyte_launch_plan, inputs={"a": 10, "b": "foobar"}, wait=True)
    assert execution.node_executions["n0"].inputs == {"a": 10}
    assert execution.node_executions["n0"].outputs == {
        "t1_int_output": 12,
        "c": "world",
    }
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


def test_monitor_workflow_execution(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_launch_plan = remote.fetch_launch_plan(name="basic.hello_world.my_wf", version=VERSION)
    execution = remote.execute(
        flyte_launch_plan,
        inputs={},
    )

    poll_interval = datetime.timedelta(seconds=1)
    time_to_give_up = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=60)

    execution = remote.sync_execution(execution, sync_nodes=True)
    while datetime.datetime.now(datetime.timezone.utc) < time_to_give_up:
        if execution.is_done:
            break

        with pytest.raises(
                FlyteAssertion, match="Please wait until the execution has completed before requesting the outputs.",
        ):
            execution.outputs

        time.sleep(poll_interval.total_seconds())
        execution = remote.sync_execution(execution, sync_nodes=True)

        if execution.node_executions:
            assert execution.node_executions["start-node"].closure.phase == 3  # SUCCEEDED

    for key in execution.node_executions:
        assert execution.node_executions[key].closure.phase == 3

    assert execution.node_executions["n0"].inputs == {}
    assert execution.node_executions["n0"].outputs["o0"] == "hello world"
    assert execution.node_executions["n0"].task_executions[0].inputs == {}
    assert execution.node_executions["n0"].task_executions[0].outputs["o0"] == "hello world"
    assert execution.inputs == {}
    assert execution.outputs["o0"] == "hello world"


def test_fetch_execute_launch_plan_with_subworkflows(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)

    flyte_launch_plan = remote.fetch_launch_plan(name="basic.subworkflows.parent_wf", version=VERSION)
    execution = remote.execute(flyte_launch_plan, inputs={"a": 101}, wait=True)
    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 101}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 103, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": 103}
    assert execution.node_executions["n1"].outputs == {"o0": "world", "o1": "world"}

    # check subworkflow task execution inputs and outputs
    subworkflow_node_executions = execution.node_executions["n1"].subworkflow_node_executions
    subworkflow_node_executions["n1-0-n0"].inputs == {"a": 103}
    subworkflow_node_executions["n1-0-n1"].outputs == {"t1_int_output": 107, "c": "world"}


def test_fetch_execute_launch_plan_with_child_workflows(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)

    flyte_launch_plan = remote.fetch_launch_plan(name="basic.child_workflow.parent_wf", version=VERSION)
    execution = remote.execute(flyte_launch_plan, inputs={"a": 3}, wait=True)

    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 3}
    assert execution.node_executions["n0"].outputs["o0"] == 6
    assert execution.node_executions["n1"].inputs == {"a": 6}
    assert execution.node_executions["n1"].outputs["o0"] == 12
    assert execution.node_executions["n2"].inputs == {"a": 6, "b": 12}
    assert execution.node_executions["n2"].outputs["o0"] == 18


def test_fetch_execute_workflow(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_workflow = remote.fetch_workflow(name="basic.hello_world.my_wf", version=VERSION)
    execution = remote.execute(flyte_workflow, inputs={}, wait=True)
    assert execution.outputs["o0"] == "hello world"
    assert isinstance(execution.closure.duration, datetime.timedelta)
    assert execution.closure.duration > datetime.timedelta(seconds=1)

    execution_to_terminate = remote.execute(flyte_workflow, {})
    remote.terminate(execution_to_terminate, cause="just because")


def test_fetch_execute_task(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_task = remote.fetch_task(name="basic.basic_workflow.t1", version=VERSION)
    execution = remote.execute(flyte_task, inputs={"a": 10}, wait=True)
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"
    assert execution.inputs["a"] == 10
    assert execution.outputs["c"] == "world"


def test_execute_python_task(register):
    """Test execution of a @task-decorated python function that is already registered."""
    from workflows.basic.basic_workflow import t1

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.execute(
        t1,
        name="basic.basic_workflow.t1",
        inputs={"a": 10},
        version=VERSION,
        wait=True,
        overwrite_cache=True,
        envs={"foo": "bar"},
        tags=["flyte"],
        cluster_pool="gpu",
    )
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"
    assert execution.spec.envs.envs == {"foo": "bar"}
    assert execution.spec.tags == ["flyte"]
    assert execution.spec.cluster_assignment.cluster_pool == "gpu"


def test_execute_python_workflow_and_launch_plan(register):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from workflows.basic.basic_workflow import my_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.execute(
        my_wf, name="basic.basic_workflow.my_wf", inputs={"a": 10, "b": "xyz"}, version=VERSION, wait=True
    )
    assert execution.outputs["o0"] == 12
    assert execution.outputs["o1"] == "xyzworld"

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(
        launch_plan,
        name="basic.basic_workflow.my_wf",
        inputs={"a": 14, "b": "foobar"},
        version=VERSION,
        wait=True,
    )
    assert execution.outputs["o0"] == 16
    assert execution.outputs["o1"] == "foobarworld"

    flyte_workflow_execution = remote.fetch_execution(name=execution.id.name)
    assert execution.inputs == flyte_workflow_execution.inputs
    assert execution.outputs == flyte_workflow_execution.outputs


def test_fetch_execute_launch_plan_list_of_floats(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_launch_plan = remote.fetch_launch_plan(name="basic.list_float_wf.my_wf", version=VERSION)
    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    execution = remote.execute(flyte_launch_plan, inputs={"xs": xs}, wait=True)
    assert execution.outputs["o0"] == "[42.24, 999.1, 0.0001]"


def test_fetch_execute_task_list_of_floats(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_task = remote.fetch_task(name="basic.list_float_wf.concat_list", version=VERSION)
    xs: typing.List[float] = [0.1, 0.2, 0.3, 0.4, -99999.7]
    execution = remote.execute(flyte_task, inputs={"xs": xs}, wait=True)
    assert execution.outputs["o0"] == "[0.1, 0.2, 0.3, 0.4, -99999.7]"


def test_fetch_execute_task_convert_dict(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_task = remote.fetch_task(
        name="basic.dict_str_wf.convert_to_string", version=VERSION
    )
    d: typing.Dict[str, str] = {"key1": "value1", "key2": "value2"}
    execution = remote.execute(flyte_task, inputs={"d": d}, wait=True)
    remote.sync_execution(execution, sync_nodes=True)
    assert json.loads(execution.outputs["o0"]) == {"key1": "value1", "key2": "value2"}


def test_execute_python_workflow_dict_of_string_to_string(register):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from workflows.basic.dict_str_wf import my_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    d: typing.Dict[str, str] = {"k1": "v1", "k2": "v2"}
    execution = remote.execute(
        my_wf,
        name="basic.dict_str_wf.my_wf",
        inputs={"d": d},
        version=VERSION,
        wait=True,
    )
    assert json.loads(execution.outputs["o0"]) == {"k1": "v1", "k2": "v2"}

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(
        launch_plan,
        name="basic.dict_str_wf.my_wf",
        inputs={"d": {"k2": "vvvv", "abc": "def"}},
        version=VERSION,
        wait=True,
    )
    assert json.loads(execution.outputs["o0"]) == {"k2": "vvvv", "abc": "def"}


def test_execute_python_workflow_list_of_floats(register):
    """Test execution of a @workflow-decorated python function and launchplan that are already registered."""
    from workflows.basic.list_float_wf import my_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)

    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    execution = remote.execute(
        my_wf,
        name="basic.list_float_wf.my_wf",
        inputs={"xs": xs},
        version=VERSION,
        wait=True,
    )
    assert execution.outputs["o0"] == "[42.24, 999.1, 0.0001]"

    launch_plan = LaunchPlan.get_or_create(workflow=my_wf, name=my_wf.name)
    execution = remote.execute(
        launch_plan,
        name="basic.list_float_wf.my_wf",
        inputs={"xs": [-1.1, 0.12345]},
        version=VERSION,
        wait=True,
    )
    assert execution.outputs["o0"] == "[-1.1, 0.12345]"


@pytest.mark.skip(reason="Waiting for https://github.com/flyteorg/flytectl/pull/440 to land")
def test_execute_sqlite3_task(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)

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
    registered_sql_task = remote.register_task(
        interactive_sql_task,
        serialization_settings=SerializationSettings(image_config=ImageConfig.auto(img_name=IMAGE)),
        version=VERSION,
    )
    execution = remote.execute(registered_sql_task, inputs={"limit": 10}, wait=True)
    output = execution.outputs["results"]
    result = output.open().all()
    assert result.__class__.__name__ == "DataFrame"
    assert "TrackId" in result
    assert "Name" in result


@pytest.mark.skip(reason="Waiting for https://github.com/flyteorg/flytectl/pull/440 to land")
def test_execute_joblib_workflow(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    flyte_workflow = remote.fetch_workflow(name="basic.joblib.joblib_workflow", version=VERSION)
    input_obj = [1, 2, 3]
    execution = remote.execute(flyte_workflow, inputs={"obj": input_obj}, wait=True)
    joblib_output = execution.outputs["o0"]
    joblib_output.download()
    output_obj = joblib.load(joblib_output.path)
    assert execution.outputs["o0"].extension() == "joblib"
    assert output_obj == input_obj


def test_execute_with_default_launch_plan(register):
    from workflows.basic.subworkflows import parent_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.execute(parent_wf, inputs={"a": 101}, version=VERSION, wait=True,
                               image_config=ImageConfig.auto(img_name=IMAGE))
    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 101}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 103, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": 103}
    assert execution.node_executions["n1"].outputs == {"o0": "world", "o1": "world"}

    # check subworkflow task execution inputs and outputs
    subworkflow_node_executions = execution.node_executions["n1"].subworkflow_node_executions
    subworkflow_node_executions["n1-0-n0"].inputs == {"a": 103}
    subworkflow_node_executions["n1-0-n1"].outputs == {"t1_int_output": 107, "c": "world"}


def test_fetch_not_exist_launch_plan(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    with pytest.raises(FlyteEntityNotExistException):
        remote.fetch_launch_plan(name="basic.list_float_wf.fake_wf", version=VERSION)


def test_execute_reference_task(register):
    nt = typing.NamedTuple("OutputsBC", [("t1_int_output", int), ("c", str)])

    @reference_task(
        project=PROJECT,
        domain=DOMAIN,
        name="basic.basic_workflow.t1",
        version=VERSION,
    )
    def t1(a: int) -> nt:
        ...

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.execute(
        t1,
        inputs={"a": 10},
        wait=True,
        overwrite_cache=True,
        envs={"foo": "bar"},
        tags=["flyte"],
        cluster_pool="gpu",
    )
    assert execution.outputs["t1_int_output"] == 12
    assert execution.outputs["c"] == "world"
    assert execution.spec.envs.envs == {"foo": "bar"}
    assert execution.spec.tags == ["flyte"]
    assert execution.spec.cluster_assignment.cluster_pool == "gpu"


def test_execute_reference_workflow(register):
    @reference_workflow(
        project=PROJECT,
        domain=DOMAIN,
        name="basic.basic_workflow.my_wf",
        version=VERSION,
    )
    def my_wf(a: int, b: str) -> (int, str):
        return a + 2, b + "world"

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.execute(
        my_wf,
        inputs={"a": 10, "b": "xyz"},
        wait=True,
        overwrite_cache=True,
        envs={"foo": "bar"},
        tags=["flyte"],
        cluster_pool="gpu",
    )
    assert execution.outputs["o0"] == 12
    assert execution.outputs["o1"] == "xyzworld"
    assert execution.spec.envs.envs == {"foo": "bar"}
    assert execution.spec.tags == ["flyte"]
    assert execution.spec.cluster_assignment.cluster_pool == "gpu"


def test_execute_reference_launchplan(register):
    @reference_launch_plan(
        project=PROJECT,
        domain=DOMAIN,
        name="basic.basic_workflow.my_wf",
        version=VERSION,
    )
    def my_wf(a: int, b: str) -> (int, str):
        return 3, "world"

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.execute(
        my_wf,
        inputs={"a": 10, "b": "xyz"},
        wait=True,
        overwrite_cache=True,
        envs={"foo": "bar"},
        tags=["flyte"],
        cluster_pool="gpu",
    )
    assert execution.outputs["o0"] == 12
    assert execution.outputs["o1"] == "xyzworld"
    assert execution.spec.envs.envs == {"foo": "bar"}
    assert execution.spec.tags == ["flyte"]
    assert execution.spec.cluster_assignment.cluster_pool == "gpu"


def test_execute_workflow_with_maptask(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    d: typing.List[int] = [1, 2, 3]
    flyte_launch_plan = remote.fetch_launch_plan(name="basic.array_map.workflow_with_maptask", version=VERSION)
    execution = remote.execute(
        flyte_launch_plan,
        inputs={"data": d, "y": 3},
        version=VERSION,
        wait=True,
    )
    assert execution.outputs["o0"] == [4, 5, 6]


@pytest.mark.lftransfers
class TestLargeFileTransfers:
    """A class to capture tests and helper functions for large file transfers."""

    @staticmethod
    def _get_minio_s3_client(remote):
        minio_s3_config = remote.file_access.data_config.s3
        sess = botocore.session.get_session()
        return sess.create_client(
            "s3",
            endpoint_url=minio_s3_config.endpoint,
            aws_access_key_id=minio_s3_config.access_key_id,
            aws_secret_access_key=minio_s3_config.secret_access_key,
        )

    @staticmethod
    def _get_s3_file_md5_bytes(s3_client, bucket, key):
        md5_hash = hashlib.md5()
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        # Read the object in chunks and update the hash (this keeps memory usage low)
        for chunk in iter(lambda: body.read(4096), b''):
            md5_hash.update(chunk)
        return md5_hash.digest()

    @staticmethod
    def _delete_s3_file(s3_client, bucket, key):
        # Delete the object
        response = s3_client.delete_object(Bucket=bucket, Key=key)
        # Ensure the object was deleted - for 'delete_object' 204 is the expected successful response code
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @staticmethod
    @contextmanager
    def _ephemeral_minio_project_domain_filename_root(s3_client, project, domain):
        """An ephemeral minio S3 path which is wiped upon the context manager's exit"""
        # Generate a random path in our Minio s3 bucket, under <BUCKET>/PROJECT/DOMAIN/<UUID>
        buckets = s3_client.list_buckets()["Buckets"]
        assert len(buckets) == 1  # We expect just the default sandbox bucket
        bucket = buckets[0]["Name"]
        root = str(uuid.uuid4())
        key = f"{PROJECT}/{DOMAIN}/{root}/"
        yield ((bucket, key), root)
        # Teardown everything under bucket/key
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
        if "Contents" in response:
            for obj in response["Contents"]:
                TestLargeFileTransfers._delete_s3_file(s3_client, bucket, obj["Key"])

    @staticmethod
    @pytest.mark.parametrize("gigabytes", [2, 3])
    def test_flyteremote_uploads_large_file(gigabytes):
        """This test checks whether FlyteRemote can upload large files."""
        remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
        minio_s3_client = TestLargeFileTransfers._get_minio_s3_client(remote)
        with ExitStack() as stack:
            # Step 1 - Create a large local file
            tempdir = stack.enter_context(tempfile.TemporaryDirectory())
            file_path = pathlib.Path(tempdir) / "large_file"

            with open(file_path, "wb") as f:
                # Write in chunks of 500mb to keep memory usage low during tests
                for _ in range(gigabytes * 2):
                    f.write(os.urandom(int(1e9 // 2)))

            # Step 2 - Create an ephemeral S3 storage location. This will be wiped
            #  on context exit to not overload the sandbox's storage
            _, ephemeral_filename_root = stack.enter_context(
                TestLargeFileTransfers._ephemeral_minio_project_domain_filename_root(
                    minio_s3_client,
                    PROJECT,
                    DOMAIN
                )
            )

            # Step 3 - Upload our large file and check whether the uploaded file's md5 checksum matches our local file's
            md5_bytes, upload_location = remote.upload_file(
                to_upload=file_path,
                project=PROJECT,
                domain=DOMAIN,
                filename_root=ephemeral_filename_root
            )

            url = urlparse(upload_location)
            bucket, key = url.netloc, url.path.lstrip("/")
            s3_md5_bytes = TestLargeFileTransfers._get_s3_file_md5_bytes(minio_s3_client, bucket, key)
            assert s3_md5_bytes == md5_bytes


def test_workflow_remote_func():
    """Test the logic of the remote execution of workflows and tasks."""
    from workflows.basic.child_workflow import parent_wf, double

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)

    out0 = remote.execute(
        double,
        inputs={"a": 3},
        wait=True,
        version=VERSION,
        image_config=ImageConfig.from_images(IMAGE),
    )
    out1 = remote.execute(
        parent_wf,
        inputs={"a": 3},
        wait=True,
        version=VERSION + "-1",
        image_config=ImageConfig.from_images(IMAGE),
    )
    out2 = remote.execute(
        parent_wf,
        inputs={"a": 2},
        wait=True,
        version=VERSION + "-2",
        image_config=ImageConfig.from_images(IMAGE),
    )

    assert out0.outputs["o0"] == 6
    assert out1.outputs["o0"] == 18
    assert out2.outputs["o0"] == 12


def test_execute_task_remote_func_list_of_floats():
    """Test remote execution of a @task-decorated python function with a list of floats."""
    from workflows.basic.list_float_wf import concat_list

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)

    xs: typing.List[float] = [0.1, 0.2, 0.3, 0.4, -99999.7]
    out = remote.execute(
        concat_list,
        inputs={"xs": xs},
        wait=True,
        version=VERSION,
        image_config=ImageConfig.from_images(IMAGE),
    )
    assert out.outputs["o0"] == "[0.1, 0.2, 0.3, 0.4, -99999.7]"


def test_execute_task_remote_func_convert_dict():
    """Test remote execution of a @task-decorated python function with a dict of strings."""
    from workflows.basic.dict_str_wf import convert_to_string

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)

    d: typing.Dict[str, str] = {"key1": "value1", "key2": "value2"}
    out = remote.execute(
        convert_to_string,
        inputs={"d": d},
        wait=True,
        version=VERSION,
        image_config=ImageConfig.from_images(IMAGE),
    )
    assert json.loads(out.outputs["o0"]) == {"key1": "value1", "key2": "value2"}


def test_execute_python_workflow_remote_func_dict_of_string_to_string():
    """Test remote execution of a @workflow-decorated python function with a dict of strings."""
    from workflows.basic.dict_str_wf import my_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)

    d: typing.Dict[str, str] = {"k1": "v1", "k2": "v2"}
    out = remote.execute(
        my_wf,
        inputs={"d": d},
        wait=True,
        version=VERSION + "dict_str_wf",
        image_config=ImageConfig.from_images(IMAGE),
    )
    assert json.loads(out.outputs["o0"]) == {"k1": "v1", "k2": "v2"}


def test_execute_python_workflow_remote_func_list_of_floats():
    """Test remote execution of a @workflow-decorated python function with a list of floats."""
    """Test execution of a @workflow-decorated python function."""
    from workflows.basic.list_float_wf import my_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)

    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    out = remote.execute(
        my_wf,
        inputs={"xs": xs},
        wait=True,
        version=VERSION + "list_float_wf",
        image_config=ImageConfig.from_images(IMAGE),
    )
    assert out.outputs["o0"] == "[42.24, 999.1, 0.0001]"


def test_execute_workflow_remote_fn_with_maptask():
    """Test remote execution of a @workflow-decorated python function with a map task."""
    from workflows.basic.array_map import workflow_with_maptask

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)

    d: typing.List[int] = [1, 2, 3]
    out = remote.execute(
        workflow_with_maptask,
        inputs={"data": d, "y": 3},
        wait=True,
        version=VERSION,
        image_config=ImageConfig.from_images(IMAGE),
    )
    assert out.outputs["o0"] == [4, 5, 6]


def test_launch_plans_registrable():
    """Test remote execution of a @workflow-decorated python function with a map task."""
    from workflows.basic.array_map import workflow_with_maptask

    from random import choice
    from string import ascii_letters

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN, interactive_mode_enabled=True)
    version = "".join(choice(ascii_letters) for _ in range(20))
    new_lp = LaunchPlan.create(name="dynamically_created_lp", workflow=workflow_with_maptask)
    remote.register_launch_plan(new_lp, version=version)


def test_register_wf_fast(register):
    from workflows.basic.subworkflows import parent_wf

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    fast_version = f"{VERSION}_fast"
    serialization_settings = SerializationSettings(image_config=ImageConfig.auto(img_name=IMAGE))
    registered_wf = remote.fast_register_workflow(parent_wf, serialization_settings, version=fast_version)
    execution = remote.execute(registered_wf, inputs={"a": 101}, wait=True)
    assert registered_wf.name == "workflows.basic.subworkflows.parent_wf"
    assert execution.spec.launch_plan.version == fast_version
    # check node execution inputs and outputs
    assert execution.node_executions["n0"].inputs == {"a": 101}
    assert execution.node_executions["n0"].outputs == {"t1_int_output": 103, "c": "world"}
    assert execution.node_executions["n1"].inputs == {"a": 103}
    assert execution.node_executions["n1"].outputs == {"o0": "world", "o1": "world"}

    # check subworkflow task execution inputs and outputs
    subworkflow_node_executions = execution.node_executions["n1"].subworkflow_node_executions
    subworkflow_node_executions["n1-0-n0"].inputs == {"a": 103}
    subworkflow_node_executions["n1-0-n1"].outputs == {"t1_int_output": 107, "c": "world"}


def test_fetch_active_launchplan_not_found(register):
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    assert remote.fetch_active_launchplan(name="basic.list_float_wf.fake_wf") is None

def test_get_control_plane_version():
    client = _SynchronousFlyteClient(PlatformConfig.for_endpoint("localhost:30080", True))
    version = client.get_control_plane_version()
    assert version == "unknown" or version.startswith("v")


def test_open_ff():
    """Test opening FlyteFile from a remote path."""
    # Upload a file to minio s3 bucket
    file_transfer = SimpleFileTransfer()
    remote_file_path = file_transfer.upload_file(file_type="json")

    execution_id = run("flytefile.py", "wf", "--remote_file_path", remote_file_path)
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    assert execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED, f"Execution failed with phase: {execution.closure.phase}"

    # Delete the remote file to free the space
    url = urlparse(remote_file_path)
    bucket, key = url.netloc, url.path.lstrip("/")
    file_transfer.delete_file(bucket=bucket, key=key)


def test_attr_access_sd():
    """Test accessing StructuredDataset attribute from a dataclass."""
    # Upload a file to minio s3 bucket
    file_transfer = SimpleFileTransfer()
    remote_file_path = file_transfer.upload_file(file_type="parquet")

    execution_id = run("attr_access_sd.py", "wf", "--uri", remote_file_path)
    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    assert execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED, f"Execution failed with phase: {execution.closure.phase}"

    # Delete the remote file to free the space
    url = urlparse(remote_file_path)
    bucket, key = url.netloc, url.path.lstrip("/")
    file_transfer.delete_file(bucket=bucket, key=key)

def test_signal_approve_reject(register):
    from flytekit.models.types import LiteralType, SimpleType
    from time import sleep

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    conditional_wf = remote.fetch_workflow(name="basic.signal_test.signal_test_wf", version=VERSION)

    execution = remote.execute(conditional_wf, inputs={"data": [1.0, 2.0, 3.0, 4.0, 5.0]})

    def retry_operation(operation):
        max_retries = 10
        for _ in range(max_retries):
            try:
                operation()
                break
            except Exception:
                sleep(1)

    retry_operation(lambda: remote.set_input("title-input", execution.id.name, value="my report", project=PROJECT, domain=DOMAIN, python_type=str, literal_type=LiteralType(simple=SimpleType.STRING)))
    retry_operation(lambda: remote.approve("review-passes", execution.id.name, project=PROJECT, domain=DOMAIN))

    remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
    assert execution.outputs["o0"] == {"title": "my report", "data": [1.0, 2.0, 3.0, 4.0, 5.0]}

    with pytest.raises(FlyteAssertion, match="Outputs could not be found because the execution ended in failure"):
        execution = remote.execute(conditional_wf, inputs={"data": [1.0, 2.0, 3.0, 4.0, 5.0]})

        retry_operation(lambda: remote.set_input("title-input", execution.id.name, value="my report", project=PROJECT, domain=DOMAIN, python_type=str, literal_type=LiteralType(simple=SimpleType.STRING)))
        retry_operation(lambda: remote.reject("review-passes", execution.id.name, project=PROJECT, domain=DOMAIN))

        remote.wait(execution=execution, timeout=datetime.timedelta(minutes=5))
        assert execution.outputs["o0"] == {"title": "my report", "data": [1.0, 2.0, 3.0, 4.0, 5.0]}

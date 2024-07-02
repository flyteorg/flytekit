import asyncio
import functools
import json as _json
import os
import shutil
import tempfile
import textwrap
import time
from collections import OrderedDict

import grpc
import mock
import pytest
import sky
from fastapi.testclient import TestClient
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.skypilot import SkyPilot, SkyPilotAgent
from flytekitplugins.skypilot.sky_server import _hostname, app, cluster_registry
from flytekitplugins.skypilot.task_utils import ContainerRunType, get_sky_task_config
from flytekitplugins.skypilot.utils import (
    BaseSkyTask,
    ClusterManager,
    ClusterStatus,
    MinimalRemoteSetting,
    SkyPathSetting,
    TaskRemotePathSetting,
    TaskStatus,
)
from requests import Response

from flytekit import Resources, task
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.extend import get_serializable
from flytekit.extend.backend.base_agent import AgentRegistry

DUMMY_TIME = 1
EXEC_TIME = 1.25
SLEEP_TIME = 1
AUTO_DOWN = 1
STOP_TIMEOUT = 1
MOCK_TASK = "mock_task"
MOCK_CLUSTER = "mock_cluster"
CORO_INTERVAL = 0.2
TIME_BUFFER = 2

test_client = TestClient(app)


@pytest.mark.parametrize("container_run_type", [(ContainerRunType.APP), (ContainerRunType.RUNTIME)])
def test_skypilot_task(container_run_type):
    from flytekitplugins.skypilot import SkyPilot, SkyPilotFunctionTask

    task_config = SkyPilot(
        cluster_name=MOCK_CLUSTER,
        setup="echo 'Hello, World!'",
        resource_config={"ordered": [{"instance_type": "e2-small"}, {"cloud": "aws"}]},
        container_run_type=container_run_type,
    )
    requests = Resources(cpu="2", mem="4Gi")
    limits = Resources(cpu="4")
    container_image = DefaultImages.default_image()
    environment = {"KEY": "value"}

    @task(
        task_config=task_config,
        requests=requests,
        limits=limits,
        container_image=container_image,
        environment=environment,
    )
    def say_hello(name: str) -> str:
        return f"Hello, {name}."

    assert say_hello.task_config == task_config
    assert say_hello.task_type == "skypilot"
    assert isinstance(say_hello, SkyPilotFunctionTask)

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    task_spec = get_serializable(OrderedDict(), serialization_settings, say_hello)
    template = task_spec.template
    container = template.container

    config = get_sky_task_config(template)
    sky_task = sky.Task.from_yaml_config(config)
    if container_run_type == ContainerRunType.APP:
        assert (
            sky_task.setup
            == textwrap.dedent(
                f"""\
            {task_config.setup}
            docker pull {container.image}
            """
            ).strip()
        )
        assert sky_task.run.startswith("docker run")

    else:
        assert (
            sky_task.setup
            == textwrap.dedent(
                f"""\
            {task_config.setup}
            """
            ).strip()
        )
        assert sky_task.run.startswith("export PYTHONPATH")

    assert container.args[0] in sky_task.run


def mock_launch(sleep_time=SLEEP_TIME):
    def wrapper(*args, **kwargs):
        time.sleep(sleep_time)

    return wrapper


async def condition_with_timeout(condition_wrapper: callable, max_allowed_time: float, error_msg: str):
    time_counter = 0
    while not condition_wrapper():
        await asyncio.sleep(0.1)
        time_counter += 0.1
        if time_counter >= max_allowed_time:
            raise Exception(error_msg)


def mock_fail(sleep_time=SLEEP_TIME):
    time.sleep(sleep_time)
    raise Exception("Mock Failure")


def mock_exec(sleep_time=SLEEP_TIME):
    time.sleep(sleep_time)
    return (0, "Mock Success")


def mock_dummy(sleep_time=SLEEP_TIME, *args, **kwargs):
    time.sleep(sleep_time)
    return (0, "Mock Success")


def mock_queue(sleep_time=SLEEP_TIME):
    time.sleep(sleep_time)
    return [{"job_name": "sky_task.mock_task", "status": sky.JobStatus.RUNNING, "job_id": 0}]


async def stop_sky_path():
    await asyncio.sleep(0.1)
    await cluster_registry.clear()


async def stop_and_wait(remote_cluster: ClusterManager, remote_task: BaseSkyTask):
    max_allowed_time = 2 * (CORO_INTERVAL) + TIME_BUFFER

    def condition_wrapper() -> bool:
        return remote_task._task_event_handler.is_terminal() and remote_task._cluster_event_handler.is_terminal()

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Task did not enter terminal state")

    max_allowed_time = AUTO_DOWN + STOP_TIMEOUT + TIME_BUFFER

    def condition_wrapper() -> bool:
        return remote_cluster._cluster_coroutine.done()

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Cluster did not finish")


@pytest.fixture
def mock_sky_queues():
    with mock.patch("flytekitplugins.skypilot.utils.NormalClusterManager.get_cluster_stopper") as mock_stop, mock.patch(
        "flytekitplugins.skypilot.utils.get_sky_autostop"
    ) as mock_autostop, mock.patch("flytekitplugins.skypilot.utils.sky.cancel") as mock_cancel, mock.patch(
        "flytekitplugins.skypilot.agent.sky.jobs.cancel"
    ) as mock_job_cancel:
        mock_stop.return_value = functools.partial(mock_exec, STOP_TIMEOUT / 2)
        mock_autostop.return_value = functools.partial(mock_exec, EXEC_TIME)
        mock_cancel.side_effect = mock_launch(sleep_time=0)
        mock_job_cancel.side_effect = mock_launch(sleep_time=0)
        yield mock_stop, mock_autostop, mock_cancel, mock_job_cancel


@pytest.fixture
def mock_fs():
    random_dir = tempfile.mkdtemp()
    raw = os.path.join(random_dir, "mock_task")
    os.makedirs(raw, exist_ok=True)
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=raw)
    fs.raw_output_fs.mkdir = mock.MagicMock(side_effect=functools.partial(os.makedirs, exist_ok=True))
    yield fs
    shutil.rmtree(random_dir)


hello0_config = SkyPilot(
    cluster_name=MOCK_CLUSTER,
    setup="echo 'Hello, World!'",
    resource_config={"ordered": [{"instance_type": "t2.small"}, {"cloud": "aws"}]},
)

hello1_config = SkyPilot(
    cluster_name=MOCK_CLUSTER,
    setup="echo 'Hello, Flyte!'",
    resource_config={"ordered": [{"instance_type": "t2.small"}, {"cloud": "aws"}]},
    task_name="say_hello1",
)


@task(
    task_config=hello0_config,
    container_image=DefaultImages.default_image(),
)
def say_hello0(name: str) -> str:
    return f"Hello, {name}."


@task(
    task_config=hello0_config,
    container_image=DefaultImages.default_image(),
)
def say_hello1(name: str) -> str:
    return f"Hello, Flyte1 {name}."


def get_container_args(mock_fs):
    return [
        "pyflyte-fast-execute",
        "--additional-distribution",
        "{{ .remote_package_path }}",
        "--dest-dir",
        "{{ .dest_dir }}",
        "--",
        "pyflyte-execute",
        "--inputs",
        "s3://becket/inputs.pb",
        "--output-prefix",
        f"{mock_fs.local_sandbox_dir}",
        "--raw-output-data-prefix",
        "s3://becket/raw" "--checkpoint-path",
        "s3://becket/checkpoint_output",
        "--prev-checkpoint",
        "s3://becket/prev_checkpoint",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "test_async_agent",
        "task-name",
        "say_hello0",
    ]


def mock_fastapi(url, json):
    if "launch" in url:
        json_setting = MinimalRemoteSetting(**json["setting"])
        asyncio.create_task(
            cluster_registry.create(
                sky.Task.from_yaml_config(json["task"]), TaskRemotePathSetting.from_minimal_setting(json_setting)
            )
        )

    # return a dummy response
    raw_resp = Response()
    raw_resp.status_code = 200
    raw_resp._content = _json.dumps({"hostname": _hostname}).encode("utf-8")
    return raw_resp


@pytest.fixture
def mock_provider(mock_fs):
    with mock.patch(
        "flytekitplugins.skypilot.agent.SkyPathSetting",
        autospec=True,
        return_value=SkyPathSetting(task_level_prefix=str(mock_fs.local_sandbox_dir), unique_id="sky_mock"),
    ) as mock_path, mock.patch(
        "flytekitplugins.skypilot.utils.NormalClusterManager.dummy_task_launcher",
        autospec=True,
        return_value=(functools.partial(mock_dummy, DUMMY_TIME)),
    ) as dummy_launch, mock.patch(
        "flytekitplugins.skypilot.utils.NormalTask.get_launch_handler",
        autospec=True,
        return_value=(functools.partial(mock_exec, EXEC_TIME)),
    ) as normal_launch, mock.patch(
        "flytekitplugins.skypilot.utils.ManagedTask.get_launch_handler",
        autospec=True,
        return_value=(functools.partial(mock_exec, EXEC_TIME)),
    ), mock.patch(
        "flytekitplugins.skypilot.utils.NormalTask.get_queue_handler",
        autospec=True,
        return_value=(functools.partial(mock_queue, EXEC_TIME)),
    ), mock.patch(
        "flytekitplugins.skypilot.utils.ManagedTask.get_queue_handler",
        autospec=True,
        return_value=(functools.partial(mock_queue, EXEC_TIME)),
    ) as managed_launch, mock.patch(
        "flytekitplugins.skypilot.utils.timeout_handler",
        autospec=True,
        side_effect=(mock_launch()),
    ) as mock_timeout, mock.patch(
        "flytekitplugins.skypilot.agent.requests.post", side_effect=test_client.post
    ), mock.patch("flytekitplugins.skypilot.agent.requests.get", side_effect=test_client.get):
        yield (mock_path, dummy_launch, normal_launch, managed_launch, mock_timeout)
        sky_path_fs = mock_path.return_value.file_access.raw_output_fs
        sky_path_fs.rm(sky_path_fs._parent(mock_path.return_value.working_dir), recursive=True)
        # cluster_registry.clear()


@pytest.fixture
def timeout_const_mock():
    with mock.patch(
        "flytekitplugins.skypilot.utils.COROUTINE_INTERVAL", CORO_INTERVAL
    ) as utils_coro_interval, mock.patch(
        "flytekitplugins.skypilot.process_utils.COROUTINE_INTERVAL", CORO_INTERVAL
    ) as process_coro_interval, mock.patch(
        "flytekitplugins.skypilot.utils.AUTO_DOWN", AUTO_DOWN
    ) as autodown_timeout, mock.patch("flytekitplugins.skypilot.utils.DOWN_TIMEOUT", STOP_TIMEOUT) as down_timeout:
        yield (utils_coro_interval, process_coro_interval, autodown_timeout, down_timeout)


def get_task_spec(task):
    serialization_settings = SerializationSettings(image_config=ImageConfig())
    task_spec = get_serializable(OrderedDict(), serialization_settings, task)
    return task_spec


@pytest.fixture
def mock_agent(mock_sky_queues):
    context = mock.MagicMock(spec=grpc.ServicerContext)
    task_spec = get_task_spec(say_hello0)
    agent = AgentRegistry.get_agent(task_spec.template.type)
    assert isinstance(agent, SkyPilotAgent)

    # sky.Task.from_yaml_config = mock.MagicMock(
    #     return_value=sky.Task.from_yaml_config({"name": f"sky_task.{MOCK_TASK}"})
    # )
    yield agent, task_spec, context


@pytest.mark.asyncio
async def test_async_agent_cancel_on_cluster_up(mock_agent, timeout_const_mock, mock_provider, mock_fs):
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )

        await asyncio.sleep(DUMMY_TIME / 2)
    resource_meta = create_task_response
    # let cluster enter up transition
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    get_task_response = await agent.get(context=context, resource_meta=resource_meta)
    phase = get_task_response.phase
    assert phase == TaskExecution.INITIALIZING
    # cancel the task
    await agent.delete(context=context, resource_meta=resource_meta)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_sky_path()


@pytest.mark.parametrize("job_launch_type, dummy_time", [(0, DUMMY_TIME), (1, 0)])
@pytest.mark.asyncio
async def test_async_agent_cancel_on_cluster_submit(
    job_launch_type, dummy_time, mock_agent, timeout_const_mock, mock_provider, mock_fs
):
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    task_spec.template.custom["job_launch_type"] = job_launch_type
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )

        await asyncio.sleep(DUMMY_TIME / 2)
        resource_meta = create_task_response
        remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]

    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    max_allowed_time = 2 * (dummy_time) + TIME_BUFFER

    # let cluster enter submit transition
    def condition_wrapper() -> bool:
        return remote_task._task_status == TaskStatus.CLUSTER_UP

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Task did not enter cluster up state")
    await agent.delete(context=context, resource_meta=resource_meta)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_sky_path()


@pytest.mark.parametrize("job_launch_type, dummy_time", [(0, DUMMY_TIME), (1, 0)])
@pytest.mark.asyncio
async def test_async_agent_cancel_on_cluster_exec(
    job_launch_type, dummy_time, mock_agent, timeout_const_mock, mock_provider, mock_fs
):
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    task_spec.template.custom["job_launch_type"] = job_launch_type
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )
    resource_meta = create_task_response
    await asyncio.sleep(DUMMY_TIME / 2)
    # let cluster enter submitted state
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    max_allowed_time = 2 * (dummy_time + EXEC_TIME + TIME_BUFFER)

    def condition_wrapper() -> bool:
        return remote_task._task_status == TaskStatus.TASK_SUBMITTED

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Task did not enter submitted state")
    await agent.delete(context=context, resource_meta=resource_meta)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_sky_path()


@pytest.mark.parametrize("job_launch_type, dummy_time", [(0, DUMMY_TIME), (1, 0)])
@pytest.mark.asyncio
async def test_async_agent_task_launch_fail(
    job_launch_type, dummy_time, mock_agent, timeout_const_mock, mock_provider, mock_fs
):
    (mock_path, dummy_launch, normal_launch, managed_launch, mock_timeout) = mock_provider
    agent, task_spec, context = mock_agent
    normal_launch.return_value = functools.partial(mock_fail, sleep_time=EXEC_TIME)
    managed_launch.return_value = functools.partial(mock_fail, sleep_time=EXEC_TIME)
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    task_spec.template.custom["job_launch_type"] = job_launch_type
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    resource_meta = create_task_response
    # let cluster enter submitted state
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    max_allowed_time = 2 * (dummy_time + EXEC_TIME + TIME_BUFFER)
    await asyncio.sleep(max_allowed_time)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_sky_path()


@pytest.mark.asyncio
async def test_async_agent_cluster_launch_fail(mock_agent, timeout_const_mock, mock_provider, mock_fs):
    (mock_path, dummy_launch, normal_launch, managed_launch, mock_timeout) = mock_provider
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        dummy_launch.return_value = functools.partial(mock_fail, sleep_time=EXEC_TIME)
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    resource_meta = create_task_response
    # let cluster enter submitted state
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    max_allowed_time = 2 * (DUMMY_TIME) + TIME_BUFFER
    await asyncio.sleep(max_allowed_time)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_sky_path()


@pytest.mark.asyncio
async def test_cluster_launch_fail_double_submit(mock_agent, timeout_const_mock, mock_provider, mock_fs):
    (mock_path, dummy_launch, normal_launch, managed_launch, mock_timeout) = mock_provider
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        dummy_launch.return_value = functools.partial(mock_fail, sleep_time=EXEC_TIME)
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    resource_meta = create_task_response
    # let cluster enter submitted state
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    max_allowed_time = 2 * (DUMMY_TIME) + TIME_BUFFER
    old_cluster_coro = remote_cluster._cluster_coroutine
    new_setup_task_spec = get_task_spec(say_hello1)
    new_setup_task_spec.template.container._args = get_container_args(mock_fs)

    def condition_wrapper() -> bool:
        return remote_cluster._cluster_status == ClusterStatus.STOPPING

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Cluster did not enter stopping state")
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        dummy_launch.return_value = functools.partial(mock_exec, sleep_time=EXEC_TIME)
        new_task_response = await agent.create(
            context=context, task_template=new_setup_task_spec.template, output_prefix=mock_fs.raw_output_prefix + "_1"
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    new_resource_meta = new_task_response
    assert remote_cluster._cluster_coroutine != old_cluster_coro
    new_remote_task = remote_cluster.tasks.get(new_resource_meta.job_name)

    def condition_wrapper() -> bool:
        return new_remote_task._task_status == TaskStatus.TASK_SUBMITTED

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Task did not enter submitted state")
    await agent.delete(context=context, resource_meta=new_resource_meta)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, new_remote_task)
    await stop_sky_path()


@pytest.mark.asyncio
async def test_cluster_dummy_double_submit(mock_agent, timeout_const_mock, mock_provider, mock_fs):
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    resource_meta = create_task_response
    # let cluster enter submitted state
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    max_allowed_time = 2 * (DUMMY_TIME) + TIME_BUFFER
    old_cluster_coro = remote_cluster._cluster_coroutine
    new_setup_task_spec = get_task_spec(say_hello1)
    new_setup_task_spec.template.container._args = get_container_args(mock_fs)

    def condition_wrapper() -> bool:
        return remote_cluster._cluster_status == ClusterStatus.DUMMY_LAUNCHING

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Cluster did not enter dummy state")
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        new_task_response = await agent.create(
            context=context, task_template=new_setup_task_spec.template, output_prefix=mock_fs.raw_output_prefix + "_1"
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    new_resource_meta = new_task_response
    assert remote_cluster._cluster_coroutine == old_cluster_coro
    new_remote_task = remote_cluster.tasks.get(new_resource_meta.job_name)

    def condition_wrapper() -> bool:
        return new_remote_task._task_status == TaskStatus.CLUSTER_UP

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Task did not enter submitted state")
    await agent.delete(context=context, resource_meta=resource_meta)
    await agent.delete(context=context, resource_meta=new_resource_meta)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_and_wait(remote_cluster, new_remote_task)
    await stop_sky_path()


@pytest.mark.asyncio
async def test_cluster_dummy_cancel_submit(mock_agent, timeout_const_mock, mock_provider, mock_fs):
    agent, task_spec, context = mock_agent
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        create_task_response = await agent.create(
            context=context, task_template=task_spec.template, output_prefix=mock_fs.raw_output_prefix
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    resource_meta = create_task_response
    # let cluster enter submitted state
    remote_cluster = cluster_registry._clusters[resource_meta.cluster_name]
    remote_task = remote_cluster.tasks.get(resource_meta.job_name)
    max_allowed_time = 2 * (DUMMY_TIME) + TIME_BUFFER
    old_cluster_coro = remote_cluster._cluster_coroutine
    await agent.delete(context=context, resource_meta=resource_meta)
    new_setup_task_spec = get_task_spec(say_hello1)
    new_setup_task_spec.template.container._args = get_container_args(mock_fs)

    def condition_wrapper() -> bool:
        return remote_cluster._cluster_status == ClusterStatus.STOPPING

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Cluster did not enter dummy state")
    with mock.patch("flytekitplugins.skypilot.agent.requests.post", side_effect=mock_fastapi):
        new_task_response = await agent.create(
            context=context, task_template=new_setup_task_spec.template, output_prefix=mock_fs.raw_output_prefix + "_1"
        )
        await asyncio.sleep(DUMMY_TIME / 2)
    new_resource_meta = new_task_response
    assert remote_cluster._cluster_coroutine != old_cluster_coro
    new_remote_task = remote_cluster.tasks.get(new_resource_meta.job_name)

    def condition_wrapper() -> bool:
        return new_remote_task._task_status == TaskStatus.CLUSTER_UP

    await condition_with_timeout(condition_wrapper, max_allowed_time, "Task did not enter submitted state")
    await agent.delete(context=context, resource_meta=new_resource_meta)
    # let loop detect the task deletion
    await stop_and_wait(remote_cluster, remote_task)
    await stop_and_wait(remote_cluster, new_remote_task)
    await stop_sky_path()

import asyncio
import functools
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
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.skypilot import SkyPilot, SkyPilotAgent
from flytekitplugins.skypilot.agent import SkyTaskFuture, SkyTaskTracker
from flytekitplugins.skypilot.task_utils import ContainerRunType, get_sky_task_config
from flytekitplugins.skypilot.utils import COROUTINE_INTERVAL, SkyPathSetting

from flytekit import Resources, task
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.extend import get_serializable
from flytekit.extend.backend.base_agent import AgentRegistry


@pytest.mark.parametrize("container_run_type", [(ContainerRunType.APP), (ContainerRunType.RUNTIME)])
def test_skypilot_task(container_run_type):
    from flytekitplugins.skypilot import SkyPilot, SkyPilotFunctionTask

    task_config = SkyPilot(
        cluster_name="mock_cluster",
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
            python -m pip uninstall flytekit -y
            python -m pip install -e /flytekit
            """
            ).strip()
        )
        assert sky_task.run.startswith("export PYTHONPATH")

    assert container.args[0] in sky_task.run


def check_task_all_done(task: SkyTaskFuture):
    assert task._launch_coro.done()
    assert task._status_upload_coro.done()
    assert task._status_check_coro.done()


def check_task_not_done(task: SkyTaskFuture):
    assert not task._launch_coro.done()
    assert not task._status_upload_coro.done()
    assert not task._status_check_coro.done()


def mock_launch(obj, sleep_time=5):
    time.sleep(sleep_time)


async def stop_sky_path():
    SkyTaskTracker._zip_coro.cancel()
    try:
        await SkyTaskTracker._zip_coro
    except asyncio.exceptions.CancelledError:
        pass


def mock_sky_queues():
    sky.status = mock.MagicMock(return_value=[{"status": sky.ClusterStatus.INIT}])
    sky.stop = mock.MagicMock()
    sky.down = mock.MagicMock()


@pytest.fixture
def mock_fs():
    random_dir = tempfile.mkdtemp()
    raw = os.path.join(random_dir, "mock_task")
    os.makedirs(raw, exist_ok=True)
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=raw)
    fs.raw_output_fs.mkdir = mock.MagicMock(side_effect=functools.partial(os.makedirs, exist_ok=True))
    yield fs
    shutil.rmtree(random_dir)


task_config = SkyPilot(
    cluster_name="mock_cluster",
    setup="echo 'Hello, World!'",
    resource_config={"image_id": "a/b:c", "ordered": [{"instance_type": "e2-small"}, {"cloud": "aws"}]},
)


@task(
    task_config=task_config,
    container_image=DefaultImages.default_image(),
)
def say_hello0(name: str) -> str:
    return f"Hello, {name}."


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
        "s3://becket",
        "--raw-output-data-prefix",
        f"{mock_fs.local_sandbox_dir}",
        "--checkpoint-path",
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


@pytest.fixture
def mock_provider(mock_fs):
    with mock.patch(
        "flytekitplugins.skypilot.agent.SkyPathSetting",
        autospec=True,
        return_value=SkyPathSetting(task_level_prefix=str(mock_fs.local_sandbox_dir), unique_id="sky_mock"),
    ) as mock_path, mock.patch(
        "flytekitplugins.skypilot.agent.setup_cloud_credential", autospec=True
    ) as cloud_setup, mock.patch(
        "flytekitplugins.skypilot.agent.SkyTaskFuture.launch",
        autospec=True,
        side_effect=functools.partial(mock_launch, sleep_time=5),
    ) as mock_provider:
        yield (mock_path, cloud_setup, mock_provider)
        sky_path_fs = SkyTaskTracker._sky_path_setting.file_access.raw_output_fs
        sky_path_fs.rm(sky_path_fs._parent(SkyTaskTracker._sky_path_setting.working_dir), recursive=True)
        SkyTaskTracker._JOB_RESIGTRY.clear()


@pytest.mark.asyncio
async def test_async_agent(mock_provider, mock_fs):
    (mock_path, cloud_setup, skypath_mock) = mock_provider
    # mock_provider.return_value = mock.MagicMock()

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    context = mock.MagicMock(spec=grpc.ServicerContext)
    mock_sky_queues()

    task_spec = get_serializable(OrderedDict(), serialization_settings, say_hello0)
    agent = AgentRegistry.get_agent(task_spec.template.type)
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)

    create_task_response = await agent.create(context=context, task_template=task_spec.template)
    resource_meta = create_task_response
    await asyncio.sleep(0)
    remote_task = SkyTaskTracker._JOB_RESIGTRY.get(resource_meta.job_name)
    check_task_not_done(remote_task)
    get_task_response = await agent.get(context=context, resource_meta=resource_meta)
    phase = get_task_response.phase
    await asyncio.sleep(0)
    assert phase == TaskExecution.INITIALIZING
    await agent.delete(context=context, resource_meta=resource_meta)
    await asyncio.sleep(COROUTINE_INTERVAL + 1)
    check_task_all_done(remote_task)
    await stop_sky_path()


@pytest.mark.asyncio
async def test_agent_coro_failed(mock_provider, mock_fs):
    (mock_path, cloud_setup, skypath_mock) = mock_provider

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    context = mock.MagicMock(spec=grpc.ServicerContext)
    mock_sky_queues()
    task_spec = get_serializable(OrderedDict(), serialization_settings, say_hello0)
    agent = AgentRegistry.get_agent(task_spec.template.type)
    assert isinstance(agent, SkyPilotAgent)
    task_spec.template.container._args = get_container_args(mock_fs)
    create_task_response = await agent.create(context=context, task_template=task_spec.template)
    resource_meta = create_task_response
    await asyncio.sleep(0)
    remote_task = SkyTaskTracker._JOB_RESIGTRY.get(resource_meta.job_name)
    get_task_response = await agent.get(context=context, resource_meta=resource_meta)
    phase = get_task_response.phase
    await asyncio.sleep(0)
    # causing task to fail
    remote_task._event_handler.failed_event.set()
    await asyncio.sleep(COROUTINE_INTERVAL + 1)
    cancel_task_response = await agent.get(context=context, resource_meta=resource_meta)
    phase = cancel_task_response.phase
    assert phase == TaskExecution.FAILED
    check_task_all_done(remote_task)
    await stop_sky_path()

import asyncio
import subprocess
from collections import OrderedDict
from shutil import which
from unittest.mock import MagicMock

import grpc
import pytest
from flyteidl.admin.agent_pb2 import PERMANENT_FAILURE, RUNNING, SUCCEEDED
from flytekit import Resources, task
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.core.resources import convert_resources_to_resource_model
from flytekit.extend import get_serializable
from flytekit.extend.backend.base_agent import AgentRegistry

from flytekitplugins.float import FloatAgent, FloatConfig, FloatTask
from flytekitplugins.float.utils import async_check_output, flyte_to_float_resources

float_missing = which("float") is None


def test_float_task():
    task_config = FloatConfig(submit_extra="--migratePolicy [enable=true]")
    requests = Resources(cpu="2", mem="4Gi")
    limits = Resources(cpu="4", mem="16Gi")
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
    assert say_hello.task_type == "float_task"
    assert isinstance(say_hello, FloatTask)

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    task_spec = get_serializable(OrderedDict(), serialization_settings, say_hello)
    template = task_spec.template
    container = template.container

    assert template.custom == {"submit_extra": "--migratePolicy [enable=true]"}
    assert container.resources == convert_resources_to_resource_model(requests=requests, limits=limits)
    assert container.image == container_image
    assert container.env == environment


def test_async_check_output():
    message = "Hello, World!"
    stdout = asyncio.run(async_check_output("echo", message))
    assert stdout.decode() == f"{message}\n"

    with pytest.raises(FileNotFoundError):
        asyncio.run(async_check_output("nonexistent_command"))

    with pytest.raises(subprocess.CalledProcessError):
        asyncio.run(async_check_output("false"))


def test_flyte_to_float_resources():
    resources = flyte_to_float_resources(convert_resources_to_resource_model(requests=Resources(), limits=Resources()))
    assert resources == (1, 1, 1, 1)

    B_IN_GIB = 1073741824
    cases = {
        ("0", "0", "0", "0"): (1, 1, 1, 1),
        ("2", "4Gi", "4", "16Gi"): (2, 4, 4, 16),
        ("4", "16Gi", "2", "4Gi"): (4, 16, 4, 16),
        ("1.1", str(B_IN_GIB + 1), "2.1", str(2 * B_IN_GIB + 1)): (2, 2, 3, 3),
    }

    for (req_cpu, req_mem, lim_cpu, lim_mem), (min_cpu, min_mem, max_cpu, max_mem) in cases.items():
        resources = flyte_to_float_resources(
            convert_resources_to_resource_model(
                requests=Resources(cpu=req_cpu, mem=req_mem),
                limits=Resources(cpu=lim_cpu, mem=lim_mem),
            )
        )
        assert resources == (min_cpu, min_mem, max_cpu, max_mem)


@pytest.mark.skipif(float_missing, reason="float binary is required")
def test_async_agent():
    task_config = FloatConfig(submit_extra="--migratePolicy [enable=true]")
    requests = Resources(cpu="2", mem="4Gi")
    limits = Resources(cpu="4", mem="16Gi")
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

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    task_spec = get_serializable(OrderedDict(), serialization_settings, say_hello)

    context = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent(context, task_spec.template.type)

    assert isinstance(agent, FloatAgent)

    create_task_response = asyncio.run(
        agent.async_create(
            context=context,
            output_prefix="",
            task_template=task_spec.template,
            inputs=None,
        )
    )
    resource_meta = create_task_response.resource_meta

    get_task_response = asyncio.run(agent.async_get(context=context, resource_meta=resource_meta))
    state = get_task_response.resource.state
    assert state in (RUNNING, SUCCEEDED)

    asyncio.run(agent.async_delete(context=context, resource_meta=resource_meta))

    get_task_response = asyncio.run(agent.async_get(context=context, resource_meta=resource_meta))
    state = get_task_response.resource.state
    assert state == PERMANENT_FAILURE

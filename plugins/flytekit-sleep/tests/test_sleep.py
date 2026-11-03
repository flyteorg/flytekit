from collections import OrderedDict
from datetime import timedelta

from flytekitplugins.sleep import Sleep
from flytekitplugins.sleep.task import SleepFunctionTask

from flytekit import task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
    env={},
)


def test_task_type():
    @task(task_config=Sleep())
    def sleep_for(duration: timedelta) -> None:
        pass

    assert isinstance(sleep_for, SleepFunctionTask)
    assert sleep_for.task_type == "core-sleep"


def test_serialization():
    @task(task_config=Sleep())
    def sleep_for(duration: timedelta) -> None:
        pass

    task_spec = get_serializable(OrderedDict(), serialization_settings, sleep_for)

    assert task_spec.template.type == "core-sleep"
    assert task_spec.template.custom == {}

    inputs = task_spec.template.interface.inputs
    assert "duration" in inputs
    assert len(inputs) == 1

    outputs = task_spec.template.interface.outputs
    assert len(outputs) == 0


def test_local_execution_calls_function():
    called = []

    @task(task_config=Sleep())
    def sleep_for(duration: timedelta) -> None:
        called.append(duration)

    sleep_for(duration=timedelta(seconds=1))
    assert called == [timedelta(seconds=1)]


def test_workflow_integration():
    @task(task_config=Sleep())
    def sleep_for(duration: timedelta) -> None:
        pass

    @workflow
    def wf(duration: timedelta) -> None:
        sleep_for(duration=duration)

    spec = get_serializable(OrderedDict(), serialization_settings, wf)
    assert len(spec.template.nodes) == 1

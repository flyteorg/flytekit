from collections import OrderedDict

from flytekitplugins.perian_job import PerianConfig, PerianTask

from flytekit import task
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable


def test_perian_task():
    task_config = PerianConfig(
        cores=2,
        memory="8",
        accelerators=1,
        accelerator_type="A100",
        country_code="DE",
    )
    container_image = DefaultImages.default_image()

    @task(
        task_config=task_config,
        container_image=container_image,
    )
    def say_hello(name: str) -> str:
        return f"Hello, {name}."

    assert say_hello.task_config == task_config
    assert say_hello.task_type == "perian_task"
    assert isinstance(say_hello, PerianTask)

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    task_spec = get_serializable(OrderedDict(), serialization_settings, say_hello)
    template = task_spec.template
    container = template.container

    assert template.custom == {
        'accelerator_type': 'A100',
        'accelerators': 1.0,
        'cores': 2.0,
        'country_code': 'DE',
        'memory': '8',
    }
    assert container.image == container_image

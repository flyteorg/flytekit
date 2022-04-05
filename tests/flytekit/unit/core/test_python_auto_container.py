from typing import Any

import pytest

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.python_auto_container import PythonAutoContainerTask, get_registerable_container_image


@pytest.fixture
def default_image_config():
    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    return ImageConfig(default_image=default_image)


@pytest.fixture
def default_serialization_settings(default_image_config):
    return SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config, env={"FOO": "bar"}
    )


def test_image_name_interpolation(default_image_config):
    img_to_interpolate = "{{.image.default.fqn}}:{{.image.default.version}}-special"
    img = get_registerable_container_image(img=img_to_interpolate, cfg=default_image_config)
    assert img == "docker.io/xyz:some-git-hash-special"


class DummyAutoContainerTask(PythonAutoContainerTask):
    def execute(self, **kwargs) -> Any:
        pass


task = DummyAutoContainerTask(name="x", task_config=None, task_type="t")


def test_default_command(default_serialization_settings):
    cmd = task.get_default_command(settings=default_serialization_settings)
    assert cmd == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--checkpoint-path",
        "{{.checkpointOutputPrefix}}",
        "--prev-checkpoint",
        "{{.prevCheckpointPrefix}}",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "tests.flytekit.unit.core.test_python_auto_container",
        "task-name",
        "task",
    ]

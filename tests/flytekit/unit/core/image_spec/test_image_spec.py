import os
from unittest.mock import Mock

import mock
import pytest

from flytekit.core import context_manager
from flytekit.core.context_manager import ExecutionState
from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import _F_IMG_ID, ImageBuildEngine
from flytekit.core.python_auto_container import update_image_spec_copy_handling
from flytekit.configuration import SerializationSettings, FastSerializationSettings, ImageConfig
from flytekit.constants import CopyFileDetection

REQUIREMENT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "requirements.txt")
REGISTRY_CONFIG_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "registry_config.json")


def test_image_spec(mock_image_spec_builder, monkeypatch):
    base_image = ImageSpec(name="base", builder="dummy", base_image="base_image")

    image_spec = ImageSpec(
        name="FLYTEKIT",
        builder="dummy",
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.9",
        registry="localhost:30001",
        base_image=base_image,
        cuda="11.2.2",
        cudnn="8",
        requirements=REQUIREMENT_FILE,
        registry_config=REGISTRY_CONFIG_FILE,
        entrypoint=["/bin/bash"],
        copy=["/src/file1.txt"]
    )
    assert image_spec._is_force_push is False

    image_spec = image_spec.with_commands("echo hello")
    image_spec = image_spec.with_packages("numpy")
    image_spec = image_spec.with_apt_packages("wget")
    image_spec = image_spec.with_copy(["/src", "/src/file2.txt"])
    image_spec = image_spec.force_push()

    assert image_spec.python_version == "3.9"
    assert image_spec.base_image == base_image
    assert image_spec.packages == ["pandas", "numpy"]
    assert image_spec.apt_packages == ["git", "wget"]
    assert image_spec.registry == "localhost:30001"
    assert image_spec.requirements == REQUIREMENT_FILE
    assert image_spec.registry_config == REGISTRY_CONFIG_FILE
    assert image_spec.cuda == "11.2.2"
    assert image_spec.cudnn == "8"
    assert image_spec.name == "flytekit"
    assert image_spec.builder == "dummy"
    assert image_spec.source_root is None
    assert image_spec.env is None
    assert image_spec.pip_index is None
    assert image_spec.is_container() is True
    assert image_spec.commands == ["echo hello"]
    assert image_spec._is_force_push is True
    assert image_spec.entrypoint == ["/bin/bash"]
    assert image_spec.copy == ["/src/file1.txt", "/src", "/src/file2.txt"]

    assert image_spec.image_name() == f"localhost:30001/flytekit:AjLtng9gJfYzLnjbNy70gA"
    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION))
    ):
        os.environ[_F_IMG_ID] = image_spec.id
        assert image_spec.is_container() is True

    ImageBuildEngine.register("dummy", mock_image_spec_builder)
    ImageBuildEngine.build(image_spec)

    assert "dummy" in ImageBuildEngine._REGISTRY
    assert image_spec.exist() is None

    # Remove the dummy builder, and build the image again
    # The image has already been built, so it shouldn't fail.
    del ImageBuildEngine._REGISTRY["dummy"]
    ImageBuildEngine.build(image_spec)

    with pytest.raises(AssertionError, match="Image builder flyte is not registered"):
        ImageBuildEngine.build(ImageSpec(builder="flyte"))

    # ImageSpec should be immutable
    image_spec.with_commands("ls")
    assert image_spec.commands == ["echo hello"]


def test_image_spec_engine_priority():
    image_spec = ImageSpec(name="FLYTEKIT")
    image_name = image_spec.image_name()

    new_image_name = f"fqn.xyz/{image_name}"
    mock_image_builder_10 = Mock()
    mock_image_builder_10.build_image.return_value = new_image_name
    mock_image_builder_default = Mock()
    mock_image_builder_default.build_image.side_effect = ValueError("should not be called")

    ImageBuildEngine.register("build_10", mock_image_builder_10, priority=10)
    ImageBuildEngine.register("build_default", mock_image_builder_default)

    ImageBuildEngine.build(image_spec)
    mock_image_builder_10.build_image.assert_called_once_with(image_spec)

    assert image_spec.image_name() == new_image_name
    del ImageBuildEngine._REGISTRY["build_10"]
    del ImageBuildEngine._REGISTRY["build_default"]


def test_build_existing_image_with_force_push():
    image_spec = ImageSpec(name="hello", builder="test").force_push()

    builder = Mock()
    builder.build_image.return_value = "fqn.xyz/new_image_name:v-test"
    ImageBuildEngine.register("test", builder)

    ImageBuildEngine.build(image_spec)
    builder.build_image.assert_called_once()


def test_custom_tag():
    spec = ImageSpec(
        name="my_image",
        python_version="3.11",
        tag_format="{spec_hash}-dev",
    )
    assert spec.image_name() == f"my_image:{spec.tag}"


@mock.patch("flytekit.image_spec.default_builder.DefaultImageBuilder.build_image")
def test_no_build_during_execution(mock_build_image):
    # Check that no builds are called during executions

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION))
    ):
        spec = ImageSpec(name="my_image_v2", python_version="3.12")
        ImageBuildEngine.build(spec)

    mock_build_image.assert_not_called()


@pytest.mark.parametrize(
    "parameter_name", [
        "packages", "conda_channels", "conda_packages",
        "apt_packages", "pip_extra_index_url", "entrypoint", "commands"
    ]
)
@pytest.mark.parametrize("value", ["requirements.txt", [1, 2, 3]])
def test_image_spec_validation_string_list(parameter_name, value):
    msg = f"{parameter_name} must be a list of strings or None"

    input_params = {parameter_name: value}

    with pytest.raises(ValueError, match=msg):
        ImageSpec(**input_params)


def test_copy_is_set_if_source_root_is_set():
    image_spec = ImageSpec(name="my_image", python_version="3.12", source_root="/tmp")
    assert image_spec.source_copy_mode == CopyFileDetection.LOADED_MODULES


def test_update_image_spec_copy_handling():
    # if fast is disabled, and copy wasn't set by the user, it should be set to python modules with source root
    image_spec = ImageSpec(name="my_image", python_version="3.12")
    assert image_spec.source_copy_mode is None
    assert image_spec.source_root is None
    ss = SerializationSettings(
        source_root="/tmp",
        fast_serialization_settings=FastSerializationSettings(
            enabled=False,
        ),
        image_config=ImageConfig.auto_default_image(),
    )
    update_image_spec_copy_handling(image_spec, ss)
    assert image_spec.source_copy_mode == CopyFileDetection.LOADED_MODULES
    assert image_spec.source_root == "/tmp"

    # specified no copy should not inherit source_root and copy shouldn't change
    image_spec = ImageSpec(name="my_image", python_version="3.12", source_copy_mode=CopyFileDetection.NO_COPY)
    assert image_spec.source_root is None
    ss = SerializationSettings(
        source_root="/tmp",
        fast_serialization_settings=FastSerializationSettings(
            enabled=False,
        ),
        image_config=ImageConfig.auto_default_image(),
    )
    update_image_spec_copy_handling(image_spec, ss)
    assert image_spec.source_copy_mode == CopyFileDetection.NO_COPY
    assert image_spec.source_root is None

    # manually specified copy should still inherit source_root
    image_spec = ImageSpec(name="my_image", python_version="3.12", source_copy_mode=CopyFileDetection.ALL)
    assert image_spec.source_root is None
    ss = SerializationSettings(
        source_root="/tmp",
        fast_serialization_settings=FastSerializationSettings(
            enabled=False,
        ),
        image_config=ImageConfig.auto_default_image(),
    )
    update_image_spec_copy_handling(image_spec, ss)
    assert image_spec.source_copy_mode == CopyFileDetection.ALL
    assert image_spec.source_root == "/tmp"

    # no fast, but because ss doesn't have source_root, it should be None
    image_spec = ImageSpec(name="my_image", python_version="3.12", source_copy_mode=None)
    assert image_spec.source_root is None
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
    )
    update_image_spec_copy_handling(image_spec, ss)
    assert image_spec.source_copy_mode is None
    assert image_spec.source_root is None


def test_registry_name():
    invalid_registry_names = [
        "invalid:port:50000",
        "ghcr.io/flyteorg:latest",
        "flyteorg:latest"
    ]
    for invalid_registry_name in invalid_registry_names:
        with pytest.raises(ValueError, match="Invalid container registry name"):
            ImageSpec(registry=invalid_registry_name)

    valid_registry_names = [
        "localhost:30000",
        "localhost:30000/flyte",
        "192.168.1.1:30000",
        "192.168.1.1:30000/myimage",
        "ghcr.io/flyteorg",
        "my.registry.com/myimage",
        "my.registry.com:5000/myimage",
        "myregistry:5000/myimage",
        "us-west1-docker.pkg.dev/example.com/my-project/my-repo"
        "flyteorg",
    ]
    for valid_registry_name in valid_registry_names:
        ImageSpec(registry=valid_registry_name)

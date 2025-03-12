import os
import sys
from unittest.mock import Mock

import mock
import pytest

import flytekit
from flytekit.configuration import ImageConfig
from flytekit.configuration.default_images import DefaultImages, PythonVersion
from flytekit.core.constants import FLYTE_INTERNAL_IMAGE_ENV_VAR


@pytest.mark.parametrize(
    "python_version_enum, expected_image_string",
    [
        (PythonVersion.PYTHON_3_9, "cr.flyte.org/flyteorg/flytekit:py3.9-latest"),
        (PythonVersion.PYTHON_3_10, "cr.flyte.org/flyteorg/flytekit:py3.10-latest"),
    ],
)
def test_defaults(python_version_enum, expected_image_string):
    assert DefaultImages.find_image_for(python_version_enum) == expected_image_string


@pytest.mark.parametrize(
    "python_version_enum, flytekit_version, expected_image_string",
    [
        (PythonVersion.PYTHON_3_9, "v0.32.0", "cr.flyte.org/flyteorg/flytekit:py3.9-0.32.0"),
    ],
)
def test_set_both(python_version_enum, flytekit_version, expected_image_string):
    assert DefaultImages.find_image_for(python_version_enum, flytekit_version) == expected_image_string


def test_image_config_auto(monkeypatch):
    x = ImageConfig.auto_default_image()
    assert x.images[0].name == "default"
    version_str = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert x.images[0].full == f"cr.flyte.org/flyteorg/flytekit:py{version_str}-latest"

    monkeypatch.setenv(FLYTE_INTERNAL_IMAGE_ENV_VAR, "test")
    assert DefaultImages.find_image_for() == "test"


def test_image_from_flytectl_config():
    image_config = ImageConfig.auto(
        config_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/sample.yaml")
    )
    assert image_config.images[0].full == "docker.io/xyz:latest"
    assert image_config.images[1].full == "docker.io/abc:None"
    assert (
        image_config.images[2].full
        == "docker.io/bcd@sha256:26c68657ccce2cb0a31b330cb0hu3b5e108d467f641c62e13ab40cbec258c68d"
    )


@mock.patch("flytekit.configuration.default_images.sys")
def test_not_version(mock_sys):
    mock_sys.version_info.major.return_value = 2
    # Python version 2 not in enum
    with pytest.raises(ValueError):
        DefaultImages.default_image()


def test_image_create():
    with pytest.raises(ValueError):
        ImageConfig.create_from("cr.flyte.org/im/g:latest")

    ic = ImageConfig.from_images("cr.flyte.org/im/g:latest")
    assert ic.default_image.fqn == "cr.flyte.org/im/g"
    assert ic.default_image.full == "cr.flyte.org/im/g:latest"

    ic = ImageConfig.from_images(
        "cr.flyte.org/im/g@sha256:26c68657ccce2cb0a31b330cb0hu3b5e108d467f641c62e13ab40cbec258c68d"
    )
    assert ic.default_image.fqn == "cr.flyte.org/im/g"
    assert (
        ic.default_image.full
        == "cr.flyte.org/im/g@sha256:26c68657ccce2cb0a31b330cb0hu3b5e108d467f641c62e13ab40cbec258c68d"
    )


def test_get_version_suffix():
    assert DefaultImages.get_version_suffix() == "latest"


def test_default_image_plugin(monkeypatch):
    new_default_image = "registry/flytekit:py3.9-latest"

    plugin_mock = Mock()
    plugin_mock.get_default_image.return_value = new_default_image
    mock_global_plugin = {"plugin": plugin_mock}
    monkeypatch.setattr(flytekit.configuration.plugin, "_GLOBAL_CONFIG", mock_global_plugin)

    assert DefaultImages.default_image() == new_default_image

import os

from flytekit.configuration import images, set_flyte_config_file


def test_load_images():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config"))
    imgs = images.get_specified_images()
    assert imgs == {"abc": "docker.io/abc", "xyz": "docker.io/xyz:latest"}


def test_no_images():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    imgs = images.get_specified_images()
    assert imgs == {}


def test_other_images():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    imgs = images.get_specified_images(other_images={"xyz": "docker.io/xyz:latest"})
    assert imgs == {"xyz": "docker.io/xyz:latest"}

import os

from flytekit.configuration import get_config_file
from flytekit.configuration.internal import Images


def test_load_images():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config"))
    imgs = Images.get_specified_images(cfg)
    assert imgs == {"abc": "docker.io/abc", "xyz": "docker.io/xyz:latest"}


def test_no_images():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    imgs = Images.get_specified_images(cfg)
    assert imgs == {}

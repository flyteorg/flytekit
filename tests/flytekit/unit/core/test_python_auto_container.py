import pytest

from flytekit.core.context_manager import ImageConfig, Image
from flytekit.core.python_auto_container import get_registerable_container_image


@pytest.fixture
def default_image_config():
    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    return ImageConfig(default_image=default_image)


def test_image_name_interpolation(default_image_config):
    img_to_interpolate = "{{.image.default.fqn}}:{{.image.default.version}}-special"
    img = get_registerable_container_image(img=img_to_interpolate, cfg=default_image_config)
    assert img == "docker.io/xyz:some-git-hash-special"

import os

import pytest

from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import (
    FLYTE_IMAGE_NAME,
    ImageBuildEngine,
    ImageSpecBuilder,
    calculate_hash_from_image_spec,
)


def test_image_spec():
    image_spec = ImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.8",
        registry="",
        base_image="cr.flyte.org/flyteorg/flytekit:py3.8-latest",
    )

    assert image_spec.python_version == "3.8"
    assert image_spec.base_image == "cr.flyte.org/flyteorg/flytekit:py3.8-latest"
    assert image_spec.packages == ["pandas"]
    assert image_spec.apt_packages == ["git"]
    assert image_spec.registry == ""
    assert image_spec.name == "flytekit"
    assert image_spec.builder == "envd"
    assert image_spec.source_root is None
    assert image_spec.env is None
    assert image_spec.is_container() is True
    assert image_spec.image_name() == "flytekit:yZ8jICcDTLoDArmNHbWNwg.."
    os.environ[FLYTE_IMAGE_NAME] = "flytekit:123"
    assert image_spec.is_container() is False

    class DummyImageSpecBuilder(ImageSpecBuilder):
        def build_image(self, img):
            ...

    ImageBuildEngine.register("dummy", DummyImageSpecBuilder())
    ImageBuildEngine._REGISTRY["dummy"].build_image(image_spec)
    assert "dummy" in ImageBuildEngine._REGISTRY
    assert calculate_hash_from_image_spec(image_spec) == "yZ8jICcDTLoDArmNHbWNwg.."

    with pytest.raises(Exception):
        image_spec.builder = "flyte"
        ImageBuildEngine.build(image_spec)

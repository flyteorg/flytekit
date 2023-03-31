import pytest

from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import build_docker_image, calculate_hash_from_image_spec, image_exist


def test_image_spec():
    image_spec = ImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.8",
        registry="",
        base_image="cr.flyte.org/flyteorg/flytekit:py3.8-latest",
    )

    with pytest.raises(NotImplementedError):
        build_docker_image(image_spec, "name", "tag", True)

    with pytest.raises(NotImplementedError):
        image_spec.build_image("name", "tag", True)

    hash_value = calculate_hash_from_image_spec(image_spec)
    assert hash_value == "KwGID--5A8Cb1SH8UUwESA.."
    assert image_exist("fake_registry", "epkr42Fd9H")

from pathlib import Path

import pytest

from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import (
    IMAGE_LOCK,
    build_docker_image,
    calculate_hash_from_image_spec,
    image_exist,
    update_lock_file,
)


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


def test_update_lock_file():
    # create a temp file
    global IMAGE_LOCK
    IMAGE_LOCK = Path("temp.lock").__str__()
    update_lock_file("registry", "tag")

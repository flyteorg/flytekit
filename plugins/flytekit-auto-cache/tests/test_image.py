import pytest # type: ignore
import hashlib
from flytekit.core.auto_cache import VersionParameters
from flytekit.image_spec.image_spec import ImageSpec
from flytekitplugins.auto_cache import CacheImage


def test_get_version_with_same_image_and_salt():
    """
    Test that calling get_version with the same image and salt returns the same hash.
    """
    cache1 = CacheImage(salt="salt")
    cache2 = CacheImage(salt="salt")

    params = VersionParameters(container_image="python:3.9")

    version1 = cache1.get_version(params)
    version2 = cache2.get_version(params)

    assert version1 == version2, f"Expected {version1}, but got {version2}"


def test_get_version_with_different_salt():
    """
    Test that calling get_version with different salts returns different hashes for the same image.
    """
    cache1 = CacheImage(salt="salt1")
    cache2 = CacheImage(salt="salt2")

    params = VersionParameters(container_image="python:3.9")

    version1 = cache1.get_version(params)
    version2 = cache2.get_version(params)

    assert version1 != version2, f"Expected different hashes but got the same: {version1}"


def test_get_version_with_different_images():
    """
    Test that different images produce different hashes.
    """
    cache = CacheImage(salt="salt")

    params1 = VersionParameters(container_image="python:3.9")
    params2 = VersionParameters(container_image="python:3.8")

    version1 = cache.get_version(params1)
    version2 = cache.get_version(params2)

    assert version1 != version2, (
        f"Hashes should be different for different images. "
        f"Got {version1} and {version2}"
    )


def test_get_version_with_image_spec():
    """
    Test that ImageSpec objects use their tag directly.
    """
    cache = CacheImage(salt="salt")

    image_spec = ImageSpec(
        name="my-image",
        registry="my-registry",
        tag="v1.0.0"
    )
    params = VersionParameters(container_image=image_spec)

    version = cache.get_version(params)
    expected = hashlib.sha256("v1.0.0".encode("utf-8")).hexdigest()
    assert version == expected, f"Expected {expected}, but got {version}"


def test_get_version_without_image():
    """
    Test that calling get_version without an image raises ValueError.
    """
    cache = CacheImage(salt="salt")
    params = VersionParameters(func=lambda x: x)  # Only providing func, no image

    with pytest.raises(ValueError, match="Image-based cache requires a container_image parameter"):
        cache.get_version(params)


def test_get_version_with_none_image():
    """
    Test that calling get_version with None image raises ValueError.
    """
    cache = CacheImage(salt="salt")
    params = VersionParameters(container_image=None)

    with pytest.raises(ValueError, match="Image-based cache requires a container_image parameter"):
        cache.get_version(params)

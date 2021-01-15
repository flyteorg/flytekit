import pytest

from flytekit import task
from flytekit.annotated.context_manager import Image, ImageConfig, RegistrationSettings
from flytekit.annotated.python_function_task import (
    PythonFunctionTask,
    get_registerable_container_image,
    isnested,
    istestfunction,
)
from tests.flytekit.unit.annotated import tasks


def foo():
    pass


def test_isnested():
    def inner_foo():
        pass

    assert isnested(foo) is False
    assert isnested(inner_foo) is True

    # Uses tasks.tasks method
    with pytest.raises(ValueError):
        tasks.tasks()


def test_istestfunction():
    assert istestfunction(foo) is True
    assert istestfunction(isnested) is False
    assert istestfunction(tasks.tasks) is False


def test_container_image_conversion():
    default_img = Image(name="default", fqn="xyz.com/abc", tag="tag1")
    other_img = Image(name="other", fqn="xyz.com/other", tag="tag-other")
    cfg = ImageConfig(default_image=default_img, images=[default_img, other_img])
    assert get_registerable_container_image(None, cfg) == "xyz.com/abc:tag1"
    assert get_registerable_container_image("", cfg) == "xyz.com/abc:tag1"
    assert get_registerable_container_image("abc", cfg) == "abc"
    assert get_registerable_container_image("abc:latest", cfg) == "abc:latest"
    assert get_registerable_container_image("abc:{{.image.default.version}}", cfg) == "abc:tag1"
    assert (
        get_registerable_container_image("{{.image.default.fqn}}:{{.image.default.version}}", cfg) == "xyz.com/abc:tag1"
    )
    assert (
        get_registerable_container_image("{{.image.other.fqn}}:{{.image.other.version}}", cfg)
        == "xyz.com/other:tag-other"
    )
    assert (
        get_registerable_container_image("{{.image.other.fqn}}:{{.image.default.version}}", cfg) == "xyz.com/other:tag1"
    )
    assert get_registerable_container_image("{{.image.other.fqn}}", cfg) == "xyz.com/other"
    # Works with images instead of just image
    assert get_registerable_container_image("{{.images.other.fqn}}", cfg) == "xyz.com/other"

    with pytest.raises(AssertionError):
        get_registerable_container_image("{{.image.blah.fqn}}:{{.image.other.version}}", cfg)

    with pytest.raises(AssertionError):
        get_registerable_container_image("{{.image.fqn}}:{{.image.other.version}}", cfg)

    with pytest.raises(AssertionError):
        get_registerable_container_image("{{.image.blah}}", cfg)


def test_py_func_task_get_container():
    def foo(i: int):
        pass

    default_img = Image(name="default", fqn="xyz.com/abc", tag="tag1")
    other_img = Image(name="other", fqn="xyz.com/other", tag="tag-other")
    cfg = ImageConfig(default_image=default_img, images=[default_img, other_img])

    settings = SerializationSettings(project="p", domain="d", version="v", image_config=cfg, env={"FOO": "bar"})

    pytask = PythonFunctionTask(None, foo, None, environment={"BAZ": "baz"})
    c = pytask.get_container(settings)
    assert c.image == "xyz.com/abc:tag1"
    assert c.env == {"FOO": "bar", "BAZ": "baz"}


def test_metadata():
    @task(cache=True, cache_version="1.0")
    def foo(i: str):
        print(f"{i}")

    metadata = foo.metadata
    assert metadata.cache is True
    assert metadata.cache_version == "1.0"

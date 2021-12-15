import pytest

from flytekit import task
from flytekit.core.context_manager import Image, ImageConfig, SerializationSettings
from flytekit.core.python_auto_container import get_registerable_container_image
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.tracker import isnested, istestfunction
from tests.flytekit.unit.core import tasks


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

    assert get_registerable_container_image("{{.image.default}}", cfg) == "xyz.com/abc:tag1"


def test_get_registerable_container_image_no_images():
    cfg = ImageConfig()

    with pytest.raises(ValueError):
        get_registerable_container_image("", cfg)


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
    # test cache, cache_serialize, and cache_version are correctly set
    @task(cache=True, cache_serialize=True, cache_version="1.0")
    def foo(i: str):
        print(f"{i}")

    foo_metadata = foo.metadata
    assert foo_metadata.cache is True
    assert foo_metadata.cache_serialize is True
    assert foo_metadata.cache_version == "1.0"

    # test cache, cache_serialize, and cache_version at no unecessarily set
    @task()
    def bar(i: str):
        print(f"{i}")

    bar_metadata = bar.metadata
    assert bar_metadata.cache is False
    assert bar_metadata.cache_serialize is False
    assert bar_metadata.cache_version == ""

    # test missing cache_version
    with pytest.raises(ValueError):

        @task(cache=True)
        def foo_missing_cache_version(i: str):
            print(f"{i}")

    # test missing cache
    with pytest.raises(ValueError):

        @task(cache_serialize=True)
        def foo_missing_cache(i: str):
            print(f"{i}")

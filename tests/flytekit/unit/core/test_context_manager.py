import os

import py
import pytest

from flytekit.configuration import secrets
from flytekit.core.context_manager import (
    ExecutionState,
    FlyteContext,
    FlyteContextManager,
    SecretsManager,
    look_up_image_info,
)


class SampleTestClass(object):
    def __init__(self, value):
        self.value = value


def test_levels():
    ctx = FlyteContextManager.current_context()
    b = ctx.new_builder()
    b.flyte_client = SampleTestClass(value=1)
    with FlyteContextManager.with_context(b) as outer:
        assert outer.flyte_client.value == 1
        b = outer.new_builder()
        b.flyte_client = SampleTestClass(value=2)
        with FlyteContextManager.with_context(b) as ctx:
            assert ctx.flyte_client.value == 2

        with FlyteContextManager.with_context(outer.with_new_compilation_state()) as ctx:
            assert ctx.flyte_client.value == 1


def test_default():
    ctx = FlyteContext.current_context()
    assert ctx.file_access is not None


def test_look_up_image_info():
    img = look_up_image_info(name="x", tag="docker.io/xyz", optional_tag=True)
    assert img.name == "x"
    assert img.tag is None
    assert img.fqn == "docker.io/xyz"

    img = look_up_image_info(name="x", tag="docker.io/xyz:latest", optional_tag=True)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "docker.io/xyz"

    img = look_up_image_info(name="x", tag="docker.io/xyz:latest", optional_tag=False)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "docker.io/xyz"

    img = look_up_image_info(name="x", tag="localhost:5000/xyz:latest", optional_tag=False)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "localhost:5000/xyz"


def test_additional_context():
    ctx = FlyteContext.current_context()
    with FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.new_execution_state().with_params(
                mode=ExecutionState.Mode.TASK_EXECUTION, additional_context={1: "outer", 2: "foo"}
            )
        )
    ) as exec_ctx_outer:
        with FlyteContextManager.with_context(
            ctx.with_execution_state(
                exec_ctx_outer.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION, additional_context={1: "inner", 3: "baz"}
                )
            )
        ) as exec_ctx_inner:
            assert exec_ctx_inner.execution_state.additional_context == {1: "inner", 2: "foo", 3: "baz"}


def test_secrets_manager_default():
    with pytest.raises(ValueError):
        sec = SecretsManager()
        sec.get("group", "key")

    with pytest.raises(ValueError):
        _ = sec.group.key


def test_secrets_manager_get_envvar():
    sec = SecretsManager()
    with pytest.raises(ValueError):
        sec.get_secrets_env_var("test", "")
    with pytest.raises(ValueError):
        sec.get_secrets_env_var("", "x")
    assert sec.get_secrets_env_var("group", "test") == f"{secrets.SECRETS_ENV_PREFIX.get()}GROUP_TEST"


def test_secrets_manager_get_file():
    sec = SecretsManager()
    with pytest.raises(ValueError):
        sec.get_secrets_file("test", "")
    with pytest.raises(ValueError):
        sec.get_secrets_file("", "x")
    assert sec.get_secrets_file("group", "test") == os.path.join(
        secrets.SECRETS_DEFAULT_DIR.get(),
        "group",
        f"{secrets.SECRETS_FILE_PREFIX.get()}test",
    )


def test_secrets_manager_file(tmpdir: py.path.local):
    tmp = tmpdir.mkdir("file_test").dirname
    os.environ["FLYTE_SECRETS_DEFAULT_DIR"] = tmp
    sec = SecretsManager()
    f = os.path.join(tmp, "test")
    with open(f, "w+") as w:
        w.write("my-password")

    with pytest.raises(ValueError):
        sec.get("test", "")
    with pytest.raises(ValueError):
        sec.get("", "x")
    # Group dir not exists
    with pytest.raises(ValueError):
        sec.get("group", "test")

    g = os.path.join(tmp, "group")
    os.makedirs(g)
    f = os.path.join(g, "test")
    with open(f, "w+") as w:
        w.write("my-password")
    assert sec.get("group", "test") == "my-password"
    assert sec.group.test == "my-password"
    del os.environ["FLYTE_SECRETS_DEFAULT_DIR"]


def test_secrets_manager_bad_env():
    with pytest.raises(ValueError):
        os.environ["TEST"] = "value"
        sec = SecretsManager()
        sec.get("group", "test")


def test_secrets_manager_env():
    sec = SecretsManager()
    os.environ[sec.get_secrets_env_var("group", "test")] = "value"
    assert sec.get("group", "test") == "value"

    os.environ[sec.get_secrets_env_var(group="group", key="key")] = "value"
    assert sec.get(group="group", key="key") == "value"

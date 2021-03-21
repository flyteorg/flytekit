import os

import py
import pytest

from flytekit.common.tasks.sdk_runnable import SecretsManager
from flytekit.configuration import secrets


def test_secrets_manager_default():
    with pytest.raises(ValueError):
        sec = SecretsManager()
        sec.get("group", "key")


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
        secrets.SECRETS_DEFAULT_DIR.get(), "group", f"{secrets.SECRETS_FILE_PREFIX.get()}test",
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

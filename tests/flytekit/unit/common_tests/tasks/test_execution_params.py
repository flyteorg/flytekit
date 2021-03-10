import os
import tempfile

import pytest

from flytekit.common.tasks.sdk_runnable import SecretsManager
from flytekit.configuration import secrets


def test_secrets_manager():
    sec = SecretsManager()
    with pytest.raises(ValueError):
        sec.get("test")

    assert sec.get_secrets_env_var("test") == f"{secrets.SECRETS_ENV_PREFIX.get()}TEST"
    assert sec.get_secrets_file("test") == os.path.join(
        secrets.SECRETS_DEFAULT_DIR.get(), secrets.SECRETS_FILE_PREFIX.get(), "test"
    )

    with pytest.raises(ValueError):
        os.environ["TEST"] = "value"
        sec.get("test")

    d = tempfile.TemporaryDirectory()
    os.environ["FLYTE_SECRETS_DEFAULT_DIR"] = d.name
    f = os.path.join(secrets.SECRETS_DEFAULT_DIR.get(), secrets.SECRETS_FILE_PREFIX.get(), "test")
    sec = SecretsManager()
    with open(f, "w+") as w:
        w.write("my-password")
    assert sec.get("test") == "my-password"
    del d

    sec = SecretsManager()
    os.environ[f"{secrets.SECRETS_ENV_PREFIX.get()}TEST"] = "value"
    assert sec.get("test") == "value"

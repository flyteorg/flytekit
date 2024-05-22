import time

import pytest

from flytekit import task
from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.remote import FlyteRemote
from flytekit.remote.entities import FlyteTask

PROJECT = "flytesnacks"
DOMAIN = "development"

TASK_NAME = "tests.flytekit.integration.remote.test_rust_remote.my_test_task"
VERSION_ID = f"{hash(time.time())}"  # Use current timestamp when initialize tests, to prevent identical re-register.


def test_register_task():
    pytest.importorskip("flyrs")

    @task()
    def my_test_task(n: int) -> int:
        return n

    remote_rs = FlyteRemote(
        Config.for_endpoint(endpoint="localhost:30080", insecure=True),
        default_project=PROJECT,
        default_domain=DOMAIN,
        enable_rust=True,
    )
    flyte_task = remote_rs.register_task(
        entity=my_test_task,
        serialization_settings=SerializationSettings(
            image_config=ImageConfig.auto(img_name="flyte-cr.io/image-name:tag")
        ),
        version=VERSION_ID,
    )
    assert isinstance(flyte_task, FlyteTask)
    assert f"{flyte_task.id}" == f"TASK:{PROJECT}:{DOMAIN}:{TASK_NAME}:{VERSION_ID}"


def test_fetch_task_without_grpc():
    pytest.importorskip("flyrs")

    remote_rs = FlyteRemote(
        Config.for_endpoint(endpoint="localhost:30080", insecure=True),
        default_project=PROJECT,
        default_domain=DOMAIN,
        enable_rust=True,
    )

    task_rs = remote_rs.fetch_task(name=TASK_NAME, version=VERSION_ID)
    assert isinstance(task_rs, FlyteTask)
    assert f"{task_rs.id}" == f"TASK:{PROJECT}:{DOMAIN}:{TASK_NAME}:{VERSION_ID}"


def test_fetch_task_and_compare():
    pytest.importorskip("flyrs")
    pytest.importorskip("grpc")

    remote_py = FlyteRemote(
        Config.for_endpoint(endpoint="localhost:30080", insecure=True), default_project=PROJECT, default_domain=DOMAIN
    )
    remote_rs = FlyteRemote(
        Config.for_endpoint(endpoint="localhost:30080", insecure=True),
        default_project=PROJECT,
        default_domain=DOMAIN,
        enable_rust=True,
    )

    task_py = remote_py.fetch_task(name=TASK_NAME, version=VERSION_ID)
    task_rs = remote_rs.fetch_task(name=TASK_NAME, version=VERSION_ID)
    assert task_py == task_rs

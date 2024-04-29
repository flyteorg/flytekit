import subprocess
import time

from mock import MagicMock

from flytekit import task
from flytekit.configuration import Config, SerializationSettings, ImageConfig
from flytekit.remote import FlyteRemote
from flytekit.remote.entities import FlyteTask

PROJECT = "flytesnacks"
DOMAIN = "development"

TASK_NAME = "test_rust_remote.my_test_task"
VERSION_ID = f"{hash(time.time())}"  # we use current timestamp when initialize tests, to prevent identical re-register.


def test_register_task():
    @task()
    def my_test_task(n: int) -> int:
        return n

    remote_rs = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN, enable_rust=True)
    mock_image_config = MagicMock(default_image=MagicMock(full="fake-cr.io/image-name:tag"))
    flyte_task = remote_rs.register_task(
        entity=my_test_task,
        # TODO: AttributeError: 'str' object has no attribute 'full' from /flytekit/core/python_auto_container.py:312: AttributeError
        # serialization_settings=SerializationSettings(image_config=ImageConfig(default_image="flyte-cr.io/image-name:tag")),
        serialization_settings=SerializationSettings(image_config=mock_image_config),
        version=VERSION_ID,
    )
    assert isinstance(flyte_task, FlyteTask)
    assert f"{flyte_task.id}" == f"TASK:{PROJECT}:{DOMAIN}:{TASK_NAME}:{VERSION_ID}"


def test_fetch_task_without_grpc():
    subprocess.run(["pip", "uninstall", "grpcio", "grpcio-status"]) 
    remote_rs = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN, enable_rust=True)

    task_rs = remote_rs.fetch_task(name=TASK_NAME, version=VERSION_ID)
    assert isinstance(task_rs, FlyteTask)
    assert f"{task_rs.id}" == f"TASK:{PROJECT}:{DOMAIN}:{TASK_NAME}:{VERSION_ID}"

    subprocess.run(["pip", "install", "grpcio", "grpcio-status"]) 
    remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN, enable_rust=False)

    task_py = remote_py.fetch_task(name=TASK_NAME, version=VERSION_ID)
    assert isinstance(task_py, FlyteTask)
    assert f"{task_py.id}" == f"TASK:{PROJECT}:{DOMAIN}:{TASK_NAME}:{VERSION_ID}"


def test_fetch_task_and_compare():
    remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
    remote_rs = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN, enable_rust=True)

    task_py = remote_py.fetch_task(name=TASK_NAME, version=VERSION_ID)
    task_rs = remote_rs.fetch_task(name=TASK_NAME, version=VERSION_ID)
    assert task_py == task_rs

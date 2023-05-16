import pytest

from flytekitplugins.kfpytorch.task import PyTorch, Worker, Master, RestartPolicy
from flytekit import Resources, task
from flytekit.configuration import Image, ImageConfig, SerializationSettings

@pytest.fixture
def serialization_settings() -> SerializationSettings:
    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    return settings


def test_pytorch_task(serialization_settings: SerializationSettings):
    @task(
        task_config=PyTorch(num_workers=10),
        cache=True,
        cache_version="1",
        requests=Resources(cpu="1"),
    )
    def my_pytorch_task(x: int, y: str) -> int:
        return x

    assert my_pytorch_task(x=10, y="hello") == 10

    assert my_pytorch_task.task_config is not None

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = serialization_settings

    assert my_pytorch_task.get_custom(settings) == {
        "workerReplicas": {
            "replicas": 10,
            "resources": {}
        },
        "masterReplicas": {
            "replicas": 1,
            "resources": {}
        },
    }
    assert my_pytorch_task.resources.limits == Resources()
    assert my_pytorch_task.resources.requests == Resources(cpu="1")
    assert my_pytorch_task.task_type == "pytorch"




def test_pytorch_task_with_default_config(serialization_settings: SerializationSettings):
    task_config = PyTorch(worker=Worker(replicas=1))

    @task(
        task_config=task_config,
        cache=True,
        requests=Resources(cpu="1"),
        cache_version="1",
    )
    def my_pytorch_task(x: int, y: str) -> int:
        return x

    assert my_pytorch_task(x=10, y="hello") == 10

    assert my_pytorch_task.task_config is not None
    assert my_pytorch_task.task_type == "pytorch"
    assert my_pytorch_task.resources.limits == Resources()
    assert my_pytorch_task.resources.requests == Resources(cpu="1")

    expected_dict = {
        "masterReplicas": {
            "replicas": 1,
            "resources": {},
        },
        "workerReplicas": {
            "replicas": 1,
            "resources": {},
        },
    }
    assert my_pytorch_task.get_custom(serialization_settings) == expected_dict


def test_pytorch_task_with_custom_config(serialization_settings: SerializationSettings):
    task_config = PyTorch(
        worker=Worker(
            replicas=5,
            requests=Resources(cpu="2", mem="2Gi"),
            limits=Resources(cpu="4", mem="2Gi"),
            image="worker:latest",
            restart_policy=RestartPolicy.FAILURE,
        ),
        master=Master(
            restart_policy=RestartPolicy.ALWAYS,
        ),
    )

    @task(
        task_config=task_config,
        cache=True,
        requests=Resources(cpu="1"),
        cache_version="1",
    )
    def my_pytorch_task(x: int, y: str) -> int:
        return x

    assert my_pytorch_task(x=10, y="hello") == 10

    assert my_pytorch_task.task_config is not None
    assert my_pytorch_task.task_type == "pytorch"
    assert my_pytorch_task.resources.limits == Resources()
    assert my_pytorch_task.resources.requests == Resources(cpu="1")

    expected_custom_dict = {
        "workerReplicas": {
            "replicas": 5,
            "image": "worker:latest",
            "resources": {
                "requests": [
                    {"name": "CPU", "value": "2"},
                    {"name": "MEMORY", "value": "2Gi"},
                ],
                "limits": [
                    {"name": "CPU", "value": "4"},
                    {"name": "MEMORY", "value": "2Gi"},
                ],
            },
            "restartPolicy": "RESTART_POLICY_ON_FAILURE",
        },
        "masterReplicas": {
            "resources": {},
            "replicas": 1,
            "restartPolicy": "RESTART_POLICY_ALWAYS",
        },
    }
    assert my_pytorch_task.get_custom(serialization_settings) == expected_custom_dict
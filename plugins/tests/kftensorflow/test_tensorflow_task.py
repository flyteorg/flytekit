from flytekitplugins.kftensorflow import TfJob

from flytekit import Resources, task
from flytekit.extend import Image, ImageConfig, SerializationSettings


def test_tensorflow_task():
    @task(
        task_config=TfJob(
            num_workers=10, per_replica_requests=Resources(cpu="1"), num_ps_replicas=1, num_chief_replicas=1
        ),
        cache=True,
        cache_version="1",
    )
    def my_tensorflow_task(x: int, y: str) -> int:
        return x

    assert my_tensorflow_task(x=10, y="hello") == 10

    assert my_tensorflow_task.task_config is not None

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert my_tensorflow_task.get_custom(settings) == {"workers": 10, "psReplicas": 1, "chiefReplicas": 1}
    assert my_tensorflow_task.resources.limits == Resources()
    assert my_tensorflow_task.resources.requests == Resources(cpu="1")
    assert my_tensorflow_task.task_type == "tensorflow"

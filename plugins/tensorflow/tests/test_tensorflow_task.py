from taskplugins import TfJob

from flytekit import task
from flytekit.annotated.context_manager import Image, ImageConfig, RegistrationSettings
from flytekit.annotated.resources import Resources


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
    reg = RegistrationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert my_tensorflow_task.get_custom(reg) == {"workers": 10, "psReplicas": 1, "chiefReplicas": 1}
    assert my_tensorflow_task.resources.limits == Resources()
    assert my_tensorflow_task.resources.requests == Resources(cpu="1")
    assert my_tensorflow_task.task_type == "tensorflow"

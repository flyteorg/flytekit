from flytekit import task
from flytekit.annotated.context_manager import RegistrationSettings, ImageConfig, Image
from flytekit.annotated.resources import Resources
from flytekit.taskplugins.pytorch.task import PyTorch


def test_pytorch_task():
    @task(task_config=PyTorch(num_workers=10, per_replica_requests=Resources(cpu="1")), cache=True)
    def my_pytorch_task(x: int, y: str) -> int:
        return x

    assert my_pytorch_task(x=10, y="hello") == 10

    assert my_pytorch_task.task_config is not None

    default_img = Image(name="default", fqn="test", tag="tag")
    reg = RegistrationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert my_pytorch_task.get_custom(reg) == {"workers": 10}
    assert my_pytorch_task.resources.limits is None
    assert my_pytorch_task.resources.requests == Resources(cpu="1")

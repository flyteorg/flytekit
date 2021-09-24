from flytekitplugins.kfmpi.task import MPIJob

from flytekit import Resources, task
from flytekit.core.context_manager import EntrypointSettings
from flytekit.extend import Image, ImageConfig, SerializationSettings


def test_mpi_task():
    @task(
        task_config=MPIJob(num_workers=10, num_launcher_replicas=10, slots=1),
        requests=Resources(cpu="1"),
        cache=True,
        cache_version="1",
    )
    def my_mpi_task(x: int, y: str) -> int:
        return x

    assert my_mpi_task(x=10, y="hello") == 10

    assert my_mpi_task.task_config is not None

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        entrypoint_settings=EntrypointSettings(path="/etc/my-entrypoint", command="my-entrypoint"),
    )

    assert my_mpi_task.get_custom(settings) == {"numLauncherReplicas": 10, "numWorkers": 10, "slots": 1}
    assert my_mpi_task.resources.limits == Resources()
    assert my_mpi_task.resources.requests == Resources(cpu="1")
    assert my_mpi_task.task_type == "mpi"

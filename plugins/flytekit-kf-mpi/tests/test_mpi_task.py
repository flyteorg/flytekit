from flytekitplugins.kfmpi.task import MPIJob, MPIJobModel

from flytekit import Resources, task
from flytekit.configuration import Image, ImageConfig, SerializationSettings


def test_mpi_model_task():
    job = MPIJobModel(
        num_workers=1,
        num_launcher_replicas=1,
        slots=1,
    )
    assert job.num_workers == 1
    assert job.num_launcher_replicas == 1
    assert job.slots == 1
    assert job.from_flyte_idl(job.to_flyte_idl())


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
    )

    assert my_mpi_task.get_custom(settings) == {"numLauncherReplicas": 10, "numWorkers": 10, "slots": 1}
    assert my_mpi_task.task_type == "mpi"

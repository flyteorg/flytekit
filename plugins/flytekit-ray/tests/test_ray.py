import pytest
import ray
from flytekitplugins.ray import RayConfig

from flytekit import PythonFunctionTask, task
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models.core.resource import ClusterSpec, HeadGroupSpec, RayCluster, WorkerGroupSpec

config = RayConfig(
    address=None,
    ray_cluster=RayCluster(
        name="test_ray",
        cluster_spec=ClusterSpec(
            head_group_spec=HeadGroupSpec(image="ray.io/ray:1.10.2"),
            worker_group_spec=[WorkerGroupSpec(group_name="test_group", replicas=3)],
        ),
    ),
    runtime_env={"pip": ["numpy"]},
)


def test_ray_task():
    @task(task_config=config)
    def t1(a: int) -> str:
        assert ray.is_initialized()
        inc = a + 2
        return str(inc)

    assert t1.task_config is not None
    assert t1.task_config == config
    assert t1.task_type == "ray"
    assert isinstance(t1, PythonFunctionTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    assert t1.get_command(settings) == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--checkpoint-path",
        "{{.checkpointOutputPrefix}}",
        "--prev-checkpoint",
        "{{.prevCheckpointPrefix}}",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "tests.test_ray",
        "task-name",
        "t1",
    ]

    assert t1(a=3) == "5"
    assert not ray.is_initialized()

    with pytest.raises(ValueError, match="You cannot specify both address and ray_cluster"):
        RayConfig(
            address="127.0.0.1",
            ray_cluster=RayCluster(
                name="test_ray",
                cluster_spec=ClusterSpec(worker_group_spec=[WorkerGroupSpec(group_name="test_group", replicas=3)]),
            ),
        )

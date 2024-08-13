import base64
import json

import ray
import yaml
from flytekitplugins.ray.models import RayCluster, RayJob, WorkerGroupSpec
from flytekitplugins.ray.task import RayJobConfig, WorkerNodeConfig
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, task
from flytekit.configuration import Image, ImageConfig, SerializationSettings

config = RayJobConfig(
    worker_node_config=[WorkerNodeConfig(group_name="test_group", replicas=3, min_replicas=0, max_replicas=10)],
    runtime_env={"pip": ["numpy"]},
    enable_autoscaling=True,
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=20,
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

    ray_job_pb = RayJob(
        ray_cluster=RayCluster(worker_group_spec=[WorkerGroupSpec("test_group", 3, 0, 10)], enable_autoscaling=True),
        runtime_env=base64.b64encode(json.dumps({"pip": ["numpy"]}).encode()).decode(),
        runtime_env_yaml=yaml.dump({"pip": ["numpy"]}),
        shutdown_after_job_finishes=True,
        ttl_seconds_after_finished=20,
    ).to_flyte_idl()

    assert t1.get_custom(settings) == MessageToDict(ray_job_pb)

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
    assert ray.is_initialized()

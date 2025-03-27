import base64
import json

import ray
import yaml

from flytekit.core.resources import pod_spec_from_resources
from flytekitplugins.ray import HeadNodeConfig
from flytekitplugins.ray.models import (
    HeadGroupSpec,
    RayCluster,
    RayJob,
    WorkerGroupSpec,
)
from flytekitplugins.ray.task import RayJobConfig, WorkerNodeConfig
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, task, PodTemplate, Resources
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models.task import K8sPod


pod_template=PodTemplate(
        primary_container_name="primary",
        labels={"lKeyA": "lValA"},
        annotations={"aKeyA": "aValA"},
    )

config = RayJobConfig(
    worker_node_config=[
        WorkerNodeConfig(
            group_name="test_group",
            replicas=3,
            min_replicas=0,
            max_replicas=10,
            pod_template=pod_template,
        )
    ],
    head_node_config=HeadNodeConfig(requests=Resources(cpu="1", mem="1Gi"), limits=Resources(cpu="2", mem="2Gi")),
    runtime_env={"pip": ["numpy"]},
    enable_autoscaling=True,
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=20,
)

default_img = Image(name="default", fqn="test", tag="tag")
settings = SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
    env={},
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

    head_pod_template = PodTemplate(
                pod_spec=pod_spec_from_resources(
                    primary_container_name="ray-head",
                    requests=Resources(cpu="1", mem="1Gi"),
                    limits=Resources(cpu="2", mem="2Gi"),
                )
            )

    ray_job_pb = RayJob(
        ray_cluster=RayCluster(
            worker_group_spec=[
                WorkerGroupSpec(
                    group_name="test_group",
                    replicas=3,
                    min_replicas=0,
                    max_replicas=10,
                    k8s_pod=K8sPod.from_pod_template(pod_template),
                )
            ],
            head_group_spec=HeadGroupSpec(k8s_pod=K8sPod.from_pod_template(head_pod_template)),
            enable_autoscaling=True,
        ),
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

existing_cluster_config = RayJobConfig(
    worker_node_config=[],
    runtime_env={"pip": ["numpy"]},
    address="localhost:8265",
)

def test_ray_task_existing_cluster():
    @task(task_config=existing_cluster_config)
    def t1(a: int) -> str:
        assert ray.is_initialized()
        inc = a + 2
        return str(inc)

    assert t1.task_config is not None
    assert t1.task_config == existing_cluster_config
    assert t1.task_type == "ray"
    assert isinstance(t1, PythonFunctionTask)

    ray_job_pb = RayJob(
        ray_cluster=RayCluster(worker_group_spec=[]),
        runtime_env=base64.b64encode(json.dumps({"pip": ["numpy"]}).encode()).decode(),
        runtime_env_yaml=yaml.dump({"pip": ["numpy"]}),
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

    # cannot execute this as it will try to hit a non-existent cluster

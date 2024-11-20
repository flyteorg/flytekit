import typing

import ray
from flytekitplugins.ray import HeadNodeConfig, RayJobConfig, WorkerNodeConfig

from flytekit import ImageSpec, Resources, task, workflow

flytekit_hash = "2778db206bbea478908c4e529dcb63cd438b6065"
flytekitplugins_ray = f"git+https://github.com/flyteorg/flytekit.git@{flytekit_hash}#subdirectory=plugins/flytekit-ray"
new_flytekit = f"git+https://github.com/flyteorg/flytekit.git@{flytekit_hash}"

container_image = ImageSpec(
    name="ray-union-demo",
    python_version="3.11.9",
    apt_packages=["wget", "gdb", "git"],
    packages=[
        new_flytekit,
        flytekitplugins_ray,
        "kubernetes",
    ],
    registry="ghcr.io/fiedlerNr9",
)
ray_config = RayJobConfig(
    head_node_config=HeadNodeConfig(
        ray_start_params={"num-cpus": "0", "log-color": "true"},
        requests=Resources(cpu="1", mem="3Gi"),
    ),
    worker_node_config=[
        WorkerNodeConfig(
            group_name="ray-group",
            replicas=0,
            min_replicas=0,
            max_replicas=2,
        )
    ],
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=120,
    enable_autoscaling=True,
)


@ray.remote
def f(x):
    return x * x


@task(
    task_config=ray_config,
    requests=Resources(mem="2Gi", cpu="3000m"),
    container_image=container_image,
)
def ray_task(n: int) -> typing.List[int]:
    futures = [f.remote(i) for i in range(n)]
    return ray.get(futures)


@workflow
def wf(n: int = 50):
    ray_task(n=n)

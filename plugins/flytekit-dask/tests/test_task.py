import pytest
from flytekitplugins.dask import Dask
from flytekitplugins.dask.task import DaskCluster, JobPodSpec

from flytekit import PythonFunctionTask, Resources, task
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


def test_dask_task_with_default_config(serialization_settings: SerializationSettings):
    task_config = Dask()

    @task(task_config=task_config)
    def dask_task():
        pass

    # Helping type completion in PyCharm
    dask_task: PythonFunctionTask[Dask]

    assert dask_task.task_config == task_config
    assert dask_task.task_type == "dask"

    expected_dict = {
        "jobPodSpec": {
            "resources": {},
        },
        "cluster": {
            "nWorkers": 1,
            "resources": {},
        },
    }
    assert dask_task.get_custom(serialization_settings) == expected_dict


def test_dask_task_get_custom(serialization_settings: SerializationSettings):
    task_config = Dask(
        job_pod_spec=JobPodSpec(
            image="job_pod_spec:latest",
            requests=Resources(cpu="1"),
            limits=Resources(cpu="2"),
        ),
        cluster=DaskCluster(
            image="dask_cluster:latest",
            n_workers=123,
            requests=Resources(cpu="3"),
            limits=Resources(cpu="4"),
        ),
    )

    @task(task_config=task_config)
    def dask_task():
        pass

    # Helping type completion in PyCharm
    dask_task: PythonFunctionTask[Dask]

    expected_custom_dict = {
        "jobPodSpec": {
            "image": "job_pod_spec:latest",
            "resources": {
                "requests": [{"name": "CPU", "value": "1"}],
                "limits": [{"name": "CPU", "value": "2"}],
            },
        },
        "cluster": {
            "nWorkers": 123,
            "image": "dask_cluster:latest",
            "resources": {
                "requests": [{"name": "CPU", "value": "3"}],
                "limits": [{"name": "CPU", "value": "4"}],
            },
        },
    }
    custom_dict = dask_task.get_custom(serialization_settings)
    assert custom_dict == expected_custom_dict

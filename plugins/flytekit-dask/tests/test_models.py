import pytest
from flytekitplugins.dask import models

from flytekit.models import task as _task


@pytest.fixture
def image() -> str:
    return "foo:latest"


@pytest.fixture
def resources() -> _task.Resources:
    return _task.Resources(
        requests=[
            _task.Resources.ResourceEntry(name=_task.Resources.ResourceName.CPU, value="3"),
        ],
        limits=[],
    )


@pytest.fixture
def default_resources() -> _task.Resources:
    return _task.Resources(requests=[], limits=[])


@pytest.fixture
def job_pod_spec(image: str, resources: _task.Resources) -> models.JobPodSpec:
    return models.JobPodSpec(image=image, resources=resources)


@pytest.fixture
def dask_cluster(image: str, resources: _task.Resources) -> models.DaskCluster:
    return models.DaskCluster(image=image, n_workers=123, resources=resources)


def test_create_job_pod_spec_to_flyte_idl_no_optional(image: str, resources: _task.Resources):
    spec = models.JobPodSpec(image=image, resources=resources)
    idl_object = spec.to_flyte_idl()
    assert idl_object.image == image
    assert idl_object.resources == resources.to_flyte_idl()


def test_create_job_pod_spec_to_flyte_idl_all_optional(default_resources: _task.Resources):
    spec = models.JobPodSpec(image=None, resources=None)
    idl_object = spec.to_flyte_idl()
    assert idl_object.image == ""
    assert idl_object.resources == default_resources.to_flyte_idl()


def test_create_job_pod_spec_property_access(image: str, resources: _task.Resources):
    spec = models.JobPodSpec(image=image, resources=resources)
    assert spec.image == image
    assert spec.resources == resources


def test_dask_cluster_to_flyte_idl_no_optional(image: str, resources: _task.Resources):
    n_workers = 1234
    cluster = models.DaskCluster(image=image, n_workers=n_workers, resources=resources)
    idl_object = cluster.to_flyte_idl()
    assert idl_object.image == image
    assert idl_object.nWorkers == n_workers
    assert idl_object.resources == resources.to_flyte_idl()


def test_dask_cluster_to_flyte_idl_all_optional(default_resources: _task.Resources):
    cluster = models.DaskCluster(image=None, n_workers=None, resources=None)
    idl_object = cluster.to_flyte_idl()
    assert idl_object.image == ""
    assert idl_object.nWorkers == 0
    assert idl_object.resources == default_resources.to_flyte_idl()


def test_dask_cluster_property_access(image: str, resources: _task.Resources):
    n_workers = 1234
    cluster = models.DaskCluster(image=image, n_workers=n_workers, resources=resources)
    assert cluster.image == image
    assert cluster.n_workers == n_workers
    assert cluster.resources == resources


def test_dask_job_to_flyte_idl_no_optional(job_pod_spec: models.JobPodSpec, dask_cluster: models.DaskCluster):
    namespace = "foobar"
    job = models.DaskJob(namespace=namespace, job_pod_spec=job_pod_spec, dask_cluster=dask_cluster)
    idl_object = job.to_flyte_idl()
    assert idl_object.namespace == namespace
    assert idl_object.jobPodSpec == job_pod_spec.to_flyte_idl()
    assert idl_object.cluster == dask_cluster.to_flyte_idl()


def test_dask_job_to_flyte_idl_all_optional(job_pod_spec: models.JobPodSpec, dask_cluster: models.DaskCluster):
    job = models.DaskJob(namespace=None, job_pod_spec=job_pod_spec, dask_cluster=dask_cluster)
    idl_object = job.to_flyte_idl()
    assert idl_object.namespace == ""


def test_dask_job_property_access(job_pod_spec: models.JobPodSpec, dask_cluster: models.DaskCluster):
    namespace = "foobar"
    job = models.DaskJob(namespace=namespace, job_pod_spec=job_pod_spec, dask_cluster=dask_cluster)
    assert job.namespace == namespace
    assert job.job_pod_spec == job_pod_spec
    assert job.dask_cluster == dask_cluster

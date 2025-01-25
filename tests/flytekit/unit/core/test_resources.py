from typing import Dict

import pytest
from kubernetes.client import V1Container, V1PodSpec, V1ResourceRequirements

import flytekit.models.task as _task_models
from flytekit import Resources
from flytekit.core.resources import (
    pod_spec_from_resources,
    convert_resources_to_resource_model,
)

_ResourceName = _task_models.Resources.ResourceName


def test_convert_no_requests_no_limits():
    resource_model = convert_resources_to_resource_model(requests=None, limits=None)
    assert isinstance(resource_model, _task_models.Resources)
    assert resource_model.requests == []
    assert resource_model.limits == []


@pytest.mark.parametrize(
    argnames=("resource_dict", "expected_resource_name"),
    argvalues=(
        ({"cpu": "2"}, _ResourceName.CPU),
        ({"mem": "1Gi"}, _ResourceName.MEMORY),
        ({"gpu": "1"}, _ResourceName.GPU),
        ({"ephemeral_storage": "123Mb"}, _ResourceName.EPHEMERAL_STORAGE),
    ),
    ids=("CPU", "MEMORY", "GPU", "EPHEMERAL_STORAGE"),
)
def test_convert_requests(resource_dict: Dict[str, str], expected_resource_name: _task_models.Resources):
    assert len(resource_dict) == 1
    expected_resource_value = list(resource_dict.values())[0]

    requests = Resources(**resource_dict)
    resources_model = convert_resources_to_resource_model(requests=requests)

    assert len(resources_model.requests) == 1
    request = resources_model.requests[0]
    assert isinstance(request, _task_models.Resources.ResourceEntry)
    assert request.name == expected_resource_name
    assert request.value == expected_resource_value
    assert len(resources_model.limits) == 0


@pytest.mark.parametrize(
    argnames=("resource_dict", "expected_resource_name"),
    argvalues=(
        ({"cpu": "2"}, _ResourceName.CPU),
        ({"mem": "1Gi"}, _ResourceName.MEMORY),
        ({"gpu": "1"}, _ResourceName.GPU),
        ({"ephemeral_storage": "123Mb"}, _ResourceName.EPHEMERAL_STORAGE),
    ),
    ids=("CPU", "MEMORY", "GPU", "EPHEMERAL_STORAGE"),
)
def test_convert_limits(resource_dict: Dict[str, str], expected_resource_name: _task_models.Resources):
    assert len(resource_dict) == 1
    expected_resource_value = list(resource_dict.values())[0]

    requests = Resources(**resource_dict)
    resources_model = convert_resources_to_resource_model(limits=requests)

    assert len(resources_model.limits) == 1
    limit = resources_model.limits[0]
    assert isinstance(limit, _task_models.Resources.ResourceEntry)
    assert limit.name == expected_resource_name
    assert limit.value == expected_resource_value
    assert len(resources_model.requests) == 0


def test_incorrect_type_resources():
    with pytest.raises(AssertionError):
        Resources(cpu=bytes(1))  # type: ignore
    with pytest.raises(AssertionError):
        Resources(mem=0.1)  # type: ignore
    with pytest.raises(AssertionError):
        Resources(gpu=0.1)  # type: ignore
    with pytest.raises(AssertionError):
        Resources(ephemeral_storage=0.1)  # type: ignore


def test_resources_serialization():
    resources = Resources(cpu="2", mem="1Gi", gpu="1", ephemeral_storage="10Gi")
    json_str = resources.to_json()
    assert isinstance(json_str, str)
    assert '"cpu": "2"' in json_str
    assert '"mem": "1Gi"' in json_str
    assert '"gpu": "1"' in json_str
    assert '"ephemeral_storage": "10Gi"' in json_str


def test_resources_deserialization():
    json_str = '{"cpu": "2", "mem": "1Gi", "gpu": "1", "ephemeral_storage": "10Gi"}'
    resources = Resources.from_json(json_str)
    assert resources.cpu == "2"
    assert resources.mem == "1Gi"
    assert resources.gpu == "1"
    assert resources.ephemeral_storage == "10Gi"


def test_resources_round_trip():
    original = Resources(cpu="4", mem="2Gi", gpu="2", ephemeral_storage="20Gi")
    json_str = original.to_json()
    result = Resources.from_json(json_str)
    assert original == result


def test_pod_spec_from_resources_requests_limits_set():
    requests = Resources(cpu="1", mem="1Gi", gpu="1", ephemeral_storage="1Gi")
    limits = Resources(cpu="4", mem="2Gi", gpu="1", ephemeral_storage="1Gi")
    primary_container_name = "foo"

    expected_pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name=primary_container_name,
                resources=V1ResourceRequirements(
                    requests={
                        "cpu": "1",
                        "memory": "1Gi",
                        "nvidia.com/gpu": "1",
                        "ephemeral-storage": "1Gi",
                    },
                    limits={
                        "cpu": "4",
                        "memory": "2Gi",
                        "nvidia.com/gpu": "1",
                        "ephemeral-storage": "1Gi",
                    },
                ),
            )
        ]
    )
    pod_spec = pod_spec_from_resources(primary_container_name=primary_container_name, requests=requests, limits=limits)
    assert expected_pod_spec == pod_spec


def test_pod_spec_from_resources_requests_set():
    requests = Resources(cpu="1", mem="1Gi")
    limits = None
    primary_container_name = "foo"

    expected_pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name=primary_container_name,
                resources=V1ResourceRequirements(
                    requests={"cpu": "1", "memory": "1Gi"},
                    limits={"cpu": "1", "memory": "1Gi"},
                ),
            )
        ]
    )
    pod_spec = pod_spec_from_resources(primary_container_name=primary_container_name, requests=requests, limits=limits)
    assert expected_pod_spec == pod_spec

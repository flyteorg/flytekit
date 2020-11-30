from flytekit.annotated.resources import get_resources


def test_get_resources():
    resources = get_resources(
        memory_request="100M",
        memory_limit="200M",
        cpu_request="1",
        cpu_limit="1.5",
        storage_request="1Gb",
        storage_limit="2Gb",
        gpu_request="1",
        gpu_limit="2",
    )
    assert resources.requests.mem == "100M"
    assert resources.limits.mem == "200M"
    assert resources.requests.cpu == "1"
    assert resources.limits.cpu == "1.5"
    assert resources.requests.storage == "1Gb"
    assert resources.limits.storage == "2Gb"
    assert resources.requests.gpu == "1"
    assert resources.limits.gpu == "2"


def test_get_resources_none_specified():
    resources = get_resources()
    assert not resources.requests.mem
    assert not resources.limits.mem
    assert not resources.requests.cpu
    assert not resources.limits.cpu
    assert not resources.requests.storage
    assert not resources.limits.storage
    assert not resources.requests.gpu
    assert not resources.limits.gpu

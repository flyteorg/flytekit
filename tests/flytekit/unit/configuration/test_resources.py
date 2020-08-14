from __future__ import absolute_import

import os

from flytekit.configuration import resources, set_flyte_config_file


def test_resource_hints_default():
    assert resources.DEFAULT_CPU_LIMIT.get() is None
    assert resources.DEFAULT_CPU_REQUEST.get() is None
    assert resources.DEFAULT_MEMORY_REQUEST.get() is None
    assert resources.DEFAULT_MEMORY_LIMIT.get() is None
    assert resources.DEFAULT_GPU_REQUEST.get() is None
    assert resources.DEFAULT_GPU_LIMIT.get() is None
    assert resources.DEFAULT_STORAGE_REQUEST.get() is None
    assert resources.DEFAULT_STORAGE_LIMIT.get() is None


def test_resource_hints():
    set_flyte_config_file(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config")
    )
    assert resources.DEFAULT_CPU_REQUEST.get() == "500m"
    assert resources.DEFAULT_CPU_LIMIT.get() == "501m"
    assert resources.DEFAULT_MEMORY_REQUEST.get() == "500Gi"
    assert resources.DEFAULT_MEMORY_LIMIT.get() == "501Gi"
    assert resources.DEFAULT_GPU_REQUEST.get() == "1"
    assert resources.DEFAULT_GPU_LIMIT.get() == "2"
    assert resources.DEFAULT_STORAGE_REQUEST.get() == "500Gi"
    assert resources.DEFAULT_STORAGE_LIMIT.get() == "501Gi"

from flytekit.configuration import common as _config_common

DEFAULT_CPU_LIMIT = _config_common.FlyteStringConfigurationEntry("resources", "default_cpu_limit")
"""
If not specified explicitly when constructing a task, this limit will be applied as the default. Follows Kubernetes CPU
request/limit format.
"""

DEFAULT_CPU_REQUEST = _config_common.FlyteStringConfigurationEntry("resources", "default_cpu_request")
"""
If not specified explicitly when constructing a task, this request will be applied as the default. Follows Kubernetes
CPU request/limit format.
"""

DEFAULT_MEMORY_LIMIT = _config_common.FlyteStringConfigurationEntry("resources", "default_memory_limit")
"""
If not specified explicitly when constructing a task, this limit will be applied as the default. Follows Kubernetes
memory request/limit format.
"""

DEFAULT_MEMORY_REQUEST = _config_common.FlyteStringConfigurationEntry("resources", "default_memory_request")
"""
If not specified explicitly when constructing a task, this request will be applied as the default. Follows Kubernetes
memory request/limit format.
"""

DEFAULT_GPU_LIMIT = _config_common.FlyteStringConfigurationEntry("resources", "default_gpu_limit")
"""
If not specified explicitly when constructing a task, this limit will be applied as the default. Follows Kubernetes GPU
request/limit format.
"""

DEFAULT_GPU_REQUEST = _config_common.FlyteStringConfigurationEntry("resources", "default_gpu_request")
"""
If not specified explicitly when constructing a task, this request will be applied as the default. Follows Kubernetes
GPU request/limit format.
"""

DEFAULT_STORAGE_LIMIT = _config_common.FlyteStringConfigurationEntry("resources", "default_storage_limit")
"""
If not specified explicitly when constructing a task, this limit will be applied as the default. Follows Kubernetes
storage request/limit format.
"""

DEFAULT_STORAGE_REQUEST = _config_common.FlyteStringConfigurationEntry("resources", "default_storage_request")
"""
If not specified explicitly when constructing a task, this request will be applied as the default. Follows Kubernetes
storage request/limit format.
"""

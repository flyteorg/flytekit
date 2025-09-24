"""
Optimized Lepton Deployment Task

Single function approach with all configuration inline for maximum optimization.
"""

from typing import Any, Dict, Optional

from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin


class LeptonEndpointTask(AsyncConnectorExecutorMixin, PythonTask):
    """Unified Lepton endpoint task for all deployment types."""

    _TASK_TYPE = "lepton_endpoint_task"

    def __init__(self, name: str, deployment_config: Dict[str, Any], **kwargs):
        # Unified interface - supports all deployment types
        inputs = {
            "endpoint_name": str,
            "deployment_type": str,  # Required: "custom", "vllm", "nim", "sglang"
            # Optional parameters with defaults from config
            "image": Optional[str],
            "port": Optional[int],
            "resource_shape": Optional[str],
            "min_replicas": Optional[int],
            "max_replicas": Optional[int],
            "node_group": Optional[str],
            # Model-specific parameters
            "checkpoint_path": Optional[str],
            "served_model_name": Optional[str],
            "tensor_parallel_size": Optional[int],
            "pipeline_parallel_size": Optional[int],
            "data_parallel_size": Optional[int],
            "extra_args": Optional[str],
        }

        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            task_config=deployment_config,
            interface=Interface(inputs=inputs, outputs={"o0": str}),
            **kwargs,
        )

    def get_custom(self, settings) -> Dict[str, Any]:
        return self.task_config or {}


def create_lepton_endpoint_task(
    deployment_type: str, name: Optional[str] = None, **config_overrides
) -> LeptonEndpointTask:
    """
    Create a Lepton deployment task with optimized inline configuration.

    Supports: custom containers, vLLM, NVIDIA NIM, SGLang deployments.
    All configuration logic is contained within this function.

    Args:
        deployment_type (str): "custom", "vllm", "nim", or "sglang"
        name (Optional[str]): Task name (auto-generated if not provided)
        **config_overrides: Configuration overrides

    Returns:
        LeptonEndpointTask: Configured task ready for deployment

    Raises:
        ValueError: If deployment_type is not supported

    Examples:
        # NIM deployment
        nim_task = create_lepton_endpoint_task("nim",
            image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
            resource_shape="gpu.2xh200")

        # Custom container
        custom_task = create_lepton_endpoint_task("custom",
            image="python:3.11-slim",
            command=["/bin/bash", "-c", "python3 -m http.server 8080"])

        # vLLM with health override
        vllm_task = create_lepton_endpoint_task("vllm",
            checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
            health_config={"liveness": {"initial_delay_seconds": 1200}})
    """
    import copy

    # Base configuration (shared by ALL deployment types)
    base = {
        "node_group": "<your-node-group>",  # Replace with your actual node group
        "health_config": {"enable_collection": True},
        "log_config": {"enable_collection": True},
        "enable_rdma": False,
        "min_replicas": 1,
        "resource_shape": "gpu.1xh200",
    }

    # GPU deployment configuration (nim/vllm/sglang only)
    gpu = {
        "port": 8000,
        "auto_scaler": {"scale_down": {"no_traffic_timeout": 3600, "scale_from_zero": False}},
        "queue_config": {"priority_class": "mid-4000"},
    }

    # Deployment-specific overrides (only unique settings)
    configs = {
        "custom": {"port": 8080, "resource_shape": "cpu.small", "max_replicas": 1},
        "vllm": {
            "image": "vllm/vllm-openai:latest",
            "max_replicas": 2,
            "tensor_parallel_size": 1,
            "pipeline_parallel_size": 1,
            "data_parallel_size": 1,
        },
        "nim": {"max_replicas": 3, "endpoint_readiness_timeout": 1800},
        "sglang": {
            "image": "lmsysorg/sglang:latest",
            "max_replicas": 2,
            "tensor_parallel_size": 1,
            "data_parallel_size": 1,
        },
    }

    if deployment_type not in configs:
        raise ValueError(f"Unsupported deployment type: {deployment_type}. Supported: {list(configs.keys())}")

    # Deep merge inline for efficiency
    def merge_configs(*dicts):
        result = copy.deepcopy(dicts[0]) if dicts else {}
        for d in dicts[1:]:
            for key, value in d.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_configs(result[key], value)
                else:
                    result[key] = copy.deepcopy(value)
        return result

    # Build final configuration
    final_configs = [base]
    if deployment_type in ("nim", "vllm", "sglang"):
        final_configs.append(gpu)
    final_configs.extend([configs[deployment_type], config_overrides])

    return LeptonEndpointTask(
        name=name or f"deploy_{deployment_type}_endpoint", deployment_config=merge_configs(*final_configs)
    )

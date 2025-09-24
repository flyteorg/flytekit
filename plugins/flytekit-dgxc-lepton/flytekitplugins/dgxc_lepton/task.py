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
            # Infrastructure parameters (commonly overridden at runtime)
            "resource_shape": str,
            "min_replicas": int,
            "max_replicas": int,
            "node_group": str,
            # Optional parameters with defaults from config
            "image": Optional[str],
            "port": Optional[int],
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
    deployment_type: str,
    name: Optional[str] = None,
    # High-level secret abstractions
    secrets: Optional[Dict[str, str]] = None,  # {"ENV_VAR_NAME": "secret_name"}
    # High-level API token abstractions
    api_token: Optional[str] = None,  # Simple token value
    api_token_secret: Optional[str] = None,  # Token from secret
    # High-level scaling abstractions
    scaling_mode: Optional[str] = None,  # "traffic", "gpu_utilization", "qpm"
    target_gpu_utilization: Optional[int] = None,
    no_traffic_timeout: Optional[int] = None,
    target_qpm: Optional[float] = None,
    scale_to_zero: bool = False,
    # High-level health abstractions
    initial_delay_seconds: Optional[int] = None,  # Liveness probe delay
    liveness_timeout: Optional[int] = None,  # Liveness probe timeout
    readiness_delay: Optional[int] = None,  # Readiness probe delay
    **config_overrides,
) -> LeptonEndpointTask:
    """
    Create a Lepton deployment task with high-level abstractions.

    Supports: custom containers, vLLM, NVIDIA NIM, SGLang deployments.
    Provides simple abstractions for common patterns.

    Args:
        deployment_type (str): "custom", "vllm", "nim", or "sglang"
        name (Optional[str]): Task name (auto-generated if not provided)
        secrets (Optional[Dict[str, str]]): Secret mapping {"ENV_VAR": "secret_name"}
        api_token (Optional[str]): Direct API token value for endpoint protection
        api_token_secret (Optional[str]): API token from Kubernetes secret
        scaling_mode (Optional[str]): "traffic", "gpu_utilization", or "qpm"
        target_gpu_utilization (Optional[int]): GPU utilization percentage (for gpu_utilization mode)
        no_traffic_timeout (Optional[int]): Seconds before scaling down (for traffic mode)
        target_qpm (Optional[float]): Queries per minute target (for qpm mode)
        scale_to_zero (bool): Allow scaling to zero replicas
        initial_delay_seconds (Optional[int]): Liveness probe initial delay
        liveness_timeout (Optional[int]): Liveness probe timeout
        readiness_delay (Optional[int]): Readiness probe initial delay
        **config_overrides: Additional configuration overrides

    Returns:
        LeptonEndpointTask: Configured task ready for deployment

    Raises:
        ValueError: If deployment_type is not supported

    Examples:
        # NIM with high-level abstractions
        nim_task = create_lepton_endpoint_task("nim",
            image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
            secrets={"HF_TOKEN": "hf-secret", "NGC_API_KEY": "ngc-secret"},
            api_token="my-unique-token",  # Simple token
            scaling_mode="gpu_utilization", target_gpu_utilization=70,
            initial_delay_seconds=5000)

        # vLLM with simple abstractions
        vllm_task = create_lepton_endpoint_task("vllm",
            checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
            secrets={"HF_TOKEN": "hf-secret"},
            api_token_secret="endpoint-token-secret",  # Token from secret
            scaling_mode="traffic", no_traffic_timeout=3600, scale_to_zero=True)
    """
    import copy

    # Base configuration (shared by ALL deployment types)
    base = {
        "health_config": {"enable_collection": True},
        "log_config": {"enable_collection": True},
        "enable_rdma": False,
        "min_replicas": 1,
        "resource_shape": "gpu.1xh200",
        "node_group": "default-node-group",  # Placeholder - override at runtime
    }

    # Advanced deployment configuration (applied to ALL deployment types)
    advanced = {
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

    # Build high-level abstractions
    high_level_config = {}

    # Auto-build env_vars from secret abstractions and merge with existing
    if secrets:
        # Start with existing env_vars if any
        env_vars = config_overrides.get("env_vars", {}).copy()
        # Add secret references
        for env_var_name, secret_name in secrets.items():
            env_vars[env_var_name] = {"value_from": {"secret_name_ref": secret_name}}
        high_level_config["env_vars"] = env_vars

    # Auto-build api_tokens from API token abstractions
    if api_token is not None and api_token_secret is not None:
        raise ValueError("Cannot specify both api_token and api_token_secret. Use one or the other.")

    if api_token:
        # Direct token value
        high_level_config["api_tokens"] = [{"value": api_token}]
    elif api_token_secret:
        # Token from secret
        high_level_config["api_tokens"] = [{"value_from": {"secret_name_ref": api_token_secret}}]

    # Auto-build auto_scaler from scaling abstractions
    if scaling_mode:
        if scaling_mode == "traffic":
            timeout = no_traffic_timeout or 3600  # Default 1 hour
            high_level_config["auto_scaler"] = {
                "scale_down": {"no_traffic_timeout": timeout, "scale_from_zero": scale_to_zero},
                "target_gpu_utilization_percentage": 0,  # Disabled
                "target_throughput": {"qpm": 0, "paths": [], "methods": []},  # Disabled
            }
        elif scaling_mode == "gpu_utilization":
            gpu_target = target_gpu_utilization or 70  # Default 70%
            high_level_config["auto_scaler"] = {
                "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
                "target_gpu_utilization_percentage": gpu_target,
                "target_throughput": {"qpm": 0, "paths": [], "methods": []},  # Disabled
            }
        elif scaling_mode == "qpm":
            qpm_target = target_qpm or 10.0  # Default 10 QPM
            high_level_config["auto_scaler"] = {
                "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
                "target_gpu_utilization_percentage": 0,  # Disabled
                "target_throughput": {"qpm": qpm_target, "paths": [], "methods": []},
            }

    # Auto-build health_config from health abstractions
    if initial_delay_seconds or liveness_timeout or readiness_delay:
        health_config = {"enable_collection": True}
        if initial_delay_seconds or liveness_timeout:
            liveness = {}
            if initial_delay_seconds:
                liveness["initial_delay_seconds"] = initial_delay_seconds
            if liveness_timeout:
                liveness["timeout_seconds"] = liveness_timeout
            health_config["liveness"] = liveness
        if readiness_delay:
            health_config["readiness"] = {"initial_delay_seconds": readiness_delay}
        high_level_config["health_config"] = health_config

    # Build final configuration
    final_configs = [base]
    final_configs.append(advanced)  # Apply advanced config to ALL deployment types
    final_configs.extend([configs[deployment_type], high_level_config, config_overrides])

    # Ensure deployment_type is included in final config
    merged_config = merge_configs(*final_configs)
    merged_config["deployment_type"] = deployment_type
    return LeptonEndpointTask(name=name or f"deploy_{deployment_type}_endpoint", deployment_config=merged_config)

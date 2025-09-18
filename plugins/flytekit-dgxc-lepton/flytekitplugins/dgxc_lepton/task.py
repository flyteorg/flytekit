from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.configuration import SerializationSettings
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.resources import Resources
from flytekit.extend import TaskPlugins
from flytekit.image_spec.image_spec import ImageSpec


@dataclass
class BaseLeptonConfig(object):
    """
    Base configuration for Lepton tasks.
    Contains common configuration shared between endpoints and batch jobs.
    """

    # Basic resource configuration
    resource_shape: str = "cpu.small"

    # Container image for the deployment/job
    image: Optional[str] = None

    # Environment variables - supports both direct values and secret references
    # Format: {"VAR_NAME": "direct_value"} or {"VAR_NAME": {"value_from": {"secret_name_ref": "SECRET_NAME"}}}
    env_vars: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None

    # Node group for deployment (for workload placement)
    node_group: Optional[str] = None

    # Storage mounts configuration
    # Format: {"enabled": True, "cache_path": "/path/to/cache", "mount_path": "/opt/cache", "storage_source": "node-nfs:lepton-shared-fs"}
    mounts: Optional[Dict[str, Any]] = None

    # Image pull secrets for private registries
    image_pull_secrets: Optional[list] = None

    # Queue configuration for task scheduling
    queue_config: Optional[Dict[str, Any]] = None

    # Additional deployment configuration
    deployment_timeout: int = 300  # seconds


@dataclass
class LeptonEndpointConfig(BaseLeptonConfig):
    """
    Configuration for Lepton endpoint tasks (inference endpoints).
    Following patterns from deployment_helpers.py and lepton_nim_llama.yaml example.
    """

    # Name of the Lepton endpoint to deploy
    endpoint_name: str = "default-endpoint"

    # Deployment type: "nim", "vllm", "sglang", or "custom" (for endpoint-based deployments)
    deployment_type: str = "custom"

    # Container port (default for inference services)
    port: int = 8080

    # Endpoint-specific replica configuration
    min_replicas: int = 1
    max_replicas: int = 1

    # Model configuration (for vLLM/SGLang deployments)
    checkpoint_path: Optional[str] = None  # HuggingFace model ID or path
    served_model_name: Optional[str] = None

    # vLLM/SGLang specific configuration (not used for NIM deployments)
    tensor_parallel_size: Optional[int] = None
    pipeline_parallel_size: Optional[int] = None  # vLLM only
    data_parallel_size: Optional[int] = None
    extra_args: Optional[str] = None  # Additional command line arguments

    # Custom command for container execution (for custom deployments)
    command: Optional[list] = None  # Command to run in the container

    # API tokens for endpoint access - supports both direct values and secret references
    # Format: [{"value": "token_string"}] or [{"value_from": {"secret_name_ref": "SECRET_NAME"}}]
    api_tokens: Optional[list] = None

    # Health check configuration
    health_check_path: str = "/health"
    health_check_initial_delay: int = 10
    health_check_period: int = 10

    # Auto-scaling configuration (following YAML pattern)
    auto_scaler: Optional[Dict[str, Any]] = None
    # Example: {
    #     "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
    #     "target_gpu_utilization_percentage": 70,
    #     "target_throughput": {"qpm": 2.5, "paths": [], "methods": []}
    # }

    # Endpoint-specific timeout
    endpoint_readiness_timeout: int = 600  # 10 minutes default


# Backward compatibility alias
LeptonConfig = LeptonEndpointConfig


class BaseLeptonTask(PythonFunctionTask):
    """
    Base class for Lepton tasks.
    Contains common functionality shared between endpoint and batch tasks.
    """

    pass


class LeptonEndpointTask(BaseLeptonTask):
    """
    A Flytekit task that deploys and runs inference on Lepton endpoints.
    """

    _TASK_TYPE = "lepton_endpoint_task"

    def __init__(
        self,
        task_config: Optional[LeptonEndpointConfig],
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        **kwargs,
    ):
        """
        Initialize a LeptonTask.

        Args:
            task_config: Configuration for the Lepton deployment
            task_function: The function to run as an inference endpoint
            container_image: Container image (optional, will use Lepton's default)
            requests: Resource requests (optional)
            limits: Resource limits (optional)
        """
        super().__init__(
            task_config=task_config or LeptonEndpointConfig(),
            task_type=self._TASK_TYPE,
            task_function=task_function,
            container_image=container_image,
            **kwargs,
        )

        # Store resource requirements for potential use
        self._requests = requests
        self._limits = limits

    def execute(self, **kwargs) -> Any:
        """
        Execute the task. For inference endpoints, this typically involves:
        1. Setting up the endpoint
        2. Running inference
        3. Returning results
        """
        # For now, delegate to parent implementation
        # In a full implementation, you might want to handle inference-specific logic here
        return PythonFunctionTask.execute(self, **kwargs)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """
        Return plugin-specific data as a serializable dictionary.
        This data will be used by the connector to create the Lepton deployment.
        Following patterns from deployment_helpers.py.
        """
        config = {
            "endpoint_name": self.task_config.endpoint_name,
            "image": self.task_config.image,
            "deployment_type": self.task_config.deployment_type,
            "port": self.task_config.port,
            "resource_shape": self.task_config.resource_shape,
            "min_replicas": self.task_config.min_replicas,
            "max_replicas": self.task_config.max_replicas,
            "env_vars": self.task_config.env_vars or {},
            "deployment_timeout": self.task_config.deployment_timeout,
            "endpoint_readiness_timeout": self.task_config.endpoint_readiness_timeout,
        }

        # Add model configuration for vLLM/SGLang deployments
        if self.task_config.checkpoint_path:
            config["checkpoint_path"] = self.task_config.checkpoint_path
        if self.task_config.served_model_name:
            config["served_model_name"] = self.task_config.served_model_name

        # Add deployment-specific configuration (only for vLLM/SGLang)
        if self.task_config.tensor_parallel_size is not None:
            config["tensor_parallel_size"] = self.task_config.tensor_parallel_size
        if self.task_config.pipeline_parallel_size is not None:
            config["pipeline_parallel_size"] = self.task_config.pipeline_parallel_size
        if self.task_config.data_parallel_size is not None:
            config["data_parallel_size"] = self.task_config.data_parallel_size

        if self.task_config.extra_args:
            config["extra_args"] = self.task_config.extra_args

        # Add custom command if specified
        if self.task_config.command:
            config["command"] = self.task_config.command

        # Add ALL optional configuration fields that connector expects

        # API tokens if specified (supports both direct values and secret references)
        if self.task_config.api_tokens:
            config["api_tokens"] = self.task_config.api_tokens

        # Node group for workload placement
        if self.task_config.node_group:
            config["node_group"] = self.task_config.node_group

        # Storage mounts (following YAML pattern)
        if self.task_config.mounts:
            config["mounts"] = self.task_config.mounts

        # Image pull secrets
        if self.task_config.image_pull_secrets:
            config["image_pull_secrets"] = self.task_config.image_pull_secrets

        # Health check configuration (only add if explicitly configured)
        if (
            self.task_config.health_check_path != "/health"
            or self.task_config.health_check_initial_delay != 10
            or self.task_config.health_check_period != 10
        ):
            config["health_check"] = {
                "path": self.task_config.health_check_path,
                "initial_delay_seconds": self.task_config.health_check_initial_delay,
                "period_seconds": self.task_config.health_check_period,
            }

        # Auto-scaler configuration (always include base config, following working JSON examples)
        if self.task_config.auto_scaler:
            config["auto_scaler"] = self.task_config.auto_scaler
        else:
            # Provide default auto_scaler config (required for Lepton endpoints)
            config["auto_scaler"] = {
                "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
                "target_gpu_utilization_percentage": 0,
                "target_throughput": {"qpm": 2.5, "paths": [], "methods": []},
            }

        # Queue configuration if specified
        if self.task_config.queue_config:
            config["queue_config"] = self.task_config.queue_config

        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)


# Register the endpoint task plugin
TaskPlugins.register_pythontask_plugin(LeptonEndpointConfig, LeptonEndpointTask)

# Backward compatibility - register the old names
TaskPlugins.register_pythontask_plugin(LeptonConfig, LeptonEndpointTask)

# Backward compatibility alias
LeptonTask = LeptonEndpointTask

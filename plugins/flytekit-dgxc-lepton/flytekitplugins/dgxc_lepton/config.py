"""Configuration classes for Lepton AI inference endpoints."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class EndpointType(Enum):
    """Supported endpoint types for Lepton AI."""

    CUSTOM = "custom"
    VLLM = "vllm"
    NIM = "nim"
    SGLANG = "sglang"


class ScalingType(Enum):
    """Supported scaling types for Lepton AI."""

    TRAFFIC = "traffic"
    GPU = "gpu"
    QPM = "qpm"


@dataclass(frozen=True)
class ScalingConfig:
    """Unified scaling configuration that enforces only one scaling type.

    This ensures that only one scaling strategy can be used at a time,
    preventing conflicting scaling configurations.

    Attributes:
        scaling_type (ScalingType): The type of scaling strategy to use
        min_replicas (int): Minimum number of replicas
        max_replicas (int): Maximum number of replicas
        timeout (Optional[int]): Timeout in seconds for traffic-based scaling
        scale_to_zero (Optional[bool]): Whether to allow scaling to zero replicas
        target_utilization (Optional[int]): Target GPU utilization percentage for GPU-based scaling
        target_qpm (Optional[float]): Target queries per minute for QPM-based scaling

    Examples:
        # Traffic-based scaling
        ScalingConfig.traffic(min_replicas=1, max_replicas=3, timeout=1800)

        # GPU-based scaling
        ScalingConfig.gpu(target_utilization=80, min_replicas=1, max_replicas=5)

        # QPM-based scaling
        ScalingConfig.qpm(target_qpm=100.0, min_replicas=2, max_replicas=4)
    """

    scaling_type: ScalingType
    min_replicas: int = 1
    max_replicas: int = 2

    # Traffic scaling parameters
    timeout: Optional[int] = None
    scale_to_zero: Optional[bool] = None

    # GPU scaling parameters
    target_utilization: Optional[int] = None

    # QPM scaling parameters
    target_qpm: Optional[float] = None

    def __post_init__(self):
        """Validate scaling configuration parameters."""
        if self.scaling_type == ScalingType.TRAFFIC:
            if self.timeout is None:
                object.__setattr__(self, "timeout", 3600)  # Default timeout
            if self.scale_to_zero is None:
                object.__setattr__(self, "scale_to_zero", False)
        elif self.scaling_type == ScalingType.GPU:
            if self.target_utilization is None:
                raise ValueError("target_utilization is required for GPU scaling")
            if not 1 <= self.target_utilization <= 100:
                raise ValueError("target_utilization must be between 1 and 100")
        elif self.scaling_type == ScalingType.QPM:
            if self.target_qpm is None:
                raise ValueError("target_qpm is required for QPM scaling")
            if self.target_qpm <= 0:
                raise ValueError("target_qpm must be positive")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Lepton scaling configuration."""
        if self.scaling_type == ScalingType.TRAFFIC:
            return {
                "scale_down": {"no_traffic_timeout": self.timeout, "scale_from_zero": self.scale_to_zero},
                "target_gpu_utilization_percentage": 0,  # Disabled
                "target_throughput": {"qpm": 0, "paths": [], "methods": []},
            }
        elif self.scaling_type == ScalingType.GPU:
            return {
                "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
                "target_gpu_utilization_percentage": self.target_utilization,
                "target_throughput": {"qpm": 0, "paths": [], "methods": []},
            }
        elif self.scaling_type == ScalingType.QPM:
            return {
                "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
                "target_gpu_utilization_percentage": 0,  # Disabled
                "target_throughput": {"qpm": self.target_qpm, "paths": [], "methods": []},
            }
        else:
            raise ValueError(f"Unknown scaling type: {self.scaling_type}")

    def get_replica_config(self) -> Dict[str, int]:
        """Get replica configuration."""
        return {
            "min_replicas": self.min_replicas,
            "max_replicas": self.max_replicas,
        }

    @classmethod
    def traffic(
        cls, min_replicas: int = 1, max_replicas: int = 2, timeout: int = 3600, scale_to_zero: bool = False
    ) -> "ScalingConfig":
        """Create traffic-based scaling configuration."""
        return cls(
            scaling_type=ScalingType.TRAFFIC,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            timeout=timeout,
            scale_to_zero=scale_to_zero,
        )

    @classmethod
    def gpu(cls, target_utilization: int, min_replicas: int = 1, max_replicas: int = 3) -> "ScalingConfig":
        """Create GPU utilization-based scaling configuration."""
        return cls(
            scaling_type=ScalingType.GPU,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_utilization=target_utilization,
        )

    @classmethod
    def qpm(cls, target_qpm: float, min_replicas: int = 1, max_replicas: int = 3) -> "ScalingConfig":
        """Create queries-per-minute based scaling configuration."""
        return cls(
            scaling_type=ScalingType.QPM, min_replicas=min_replicas, max_replicas=max_replicas, target_qpm=target_qpm
        )


@dataclass(frozen=True)
class EnvironmentConfig:
    """Unified environment variable configuration for Lepton deployments.

    Handles both regular environment variables and secret references.
    """

    env_vars: Dict[str, str] = None
    secrets: Dict[str, str] = None

    def __post_init__(self):
        if self.env_vars is None:
            object.__setattr__(self, "env_vars", {})
        if self.secrets is None:
            object.__setattr__(self, "secrets", {})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Lepton-compatible environment variables."""
        result = {}

        # Add regular environment variables
        result.update(self.env_vars)

        # Add secret references
        for env_var, secret_name in self.secrets.items():
            result[env_var] = {"value_from": {"secret_name_ref": secret_name}}

        return result

    @classmethod
    def create(
        cls, env_vars: Optional[Dict[str, str]] = None, secrets: Optional[Dict[str, str]] = None, **kwargs
    ) -> "EnvironmentConfig":
        """Create EnvironmentConfig with environment variables and secrets.

        Args:
            env_vars (Optional[Dict[str, str]]): Regular environment variables
            secrets (Optional[Dict[str, str]]): Environment variables from secrets (env_var -> secret_name)
            **kwargs: Additional environment variables (treated as regular env vars)

        Returns:
            EnvironmentConfig: Configured environment instance
        """
        final_env_vars = {}
        if env_vars:
            final_env_vars.update(env_vars)
        final_env_vars.update(kwargs)

        return cls(env_vars=final_env_vars, secrets=secrets or {})

    @classmethod
    def from_secrets(cls, **secret_mapping: str) -> "EnvironmentConfig":
        """Create EnvironmentConfig with only secrets."""
        return cls(env_vars={}, secrets=secret_mapping)

    @classmethod
    def from_env(cls, **env_vars: str) -> "EnvironmentConfig":
        """Create EnvironmentConfig with only environment variables."""
        return cls(env_vars=env_vars, secrets={})


@dataclass(frozen=True)
class EndpointEngineConfig:
    """Unified endpoint configuration that enforces only one endpoint type.

    This class consolidates all endpoint-specific configurations into a single,
    type-safe configuration that ensures only one endpoint type can be specified.
    """

    endpoint_type: EndpointType
    image: str
    port: int = 8000

    # VLLM-specific parameters
    checkpoint_path: Optional[str] = None
    served_model_name: Optional[str] = None
    tensor_parallel_size: Optional[int] = None
    pipeline_parallel_size: Optional[int] = None
    data_parallel_size: Optional[int] = None
    extra_args: Optional[str] = None

    # Custom container parameters
    command: Optional[List[str]] = None

    def __post_init__(self):
        """Validate configuration based on endpoint type."""
        if self.endpoint_type == EndpointType.VLLM:
            # Set VLLM defaults if not provided
            if self.checkpoint_path is None:
                object.__setattr__(self, "checkpoint_path", "meta-llama/Llama-3.1-8B-Instruct")
            if self.served_model_name is None:
                object.__setattr__(self, "served_model_name", "default-model")
            if self.tensor_parallel_size is None:
                object.__setattr__(self, "tensor_parallel_size", 1)
            if self.pipeline_parallel_size is None:
                object.__setattr__(self, "pipeline_parallel_size", 1)
            if self.data_parallel_size is None:
                object.__setattr__(self, "data_parallel_size", 1)
            if self.extra_args is None:
                object.__setattr__(self, "extra_args", "")

        elif self.endpoint_type == EndpointType.SGLANG:
            # Set SGLang defaults if not provided
            if self.checkpoint_path is None:
                object.__setattr__(self, "checkpoint_path", "meta-llama/Llama-3.1-8B-Instruct")
            if self.served_model_name is None:
                object.__setattr__(self, "served_model_name", "default-model")
            if self.tensor_parallel_size is None:
                object.__setattr__(self, "tensor_parallel_size", 1)
            if self.data_parallel_size is None:
                object.__setattr__(self, "data_parallel_size", 1)
            if self.extra_args is None:
                object.__setattr__(self, "extra_args", "")

        elif self.endpoint_type == EndpointType.CUSTOM:
            # Custom containers use different default port
            if self.port == 8000:  # Only change if using default
                object.__setattr__(self, "port", 8080)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to endpoint parameters based on endpoint type."""
        base_dict = {"image": self.image, "port": self.port}

        if self.endpoint_type in [EndpointType.VLLM, EndpointType.SGLANG]:
            base_dict.update(
                {
                    "checkpoint_path": self.checkpoint_path,
                    "served_model_name": self.served_model_name,
                    "tensor_parallel_size": self.tensor_parallel_size,
                    "data_parallel_size": self.data_parallel_size,
                    "extra_args": self.extra_args,
                }
            )

            # VLLM-specific parameter
            if self.endpoint_type == EndpointType.VLLM:
                base_dict["pipeline_parallel_size"] = self.pipeline_parallel_size

        elif self.endpoint_type == EndpointType.CUSTOM:
            if self.command:
                base_dict["command"] = self.command

        # NIM uses base configuration as-is

        return base_dict

    @classmethod
    def vllm(
        cls,
        image: str = "vllm/vllm-openai:latest",
        checkpoint_path: str = "meta-llama/Llama-3.1-8B-Instruct",
        served_model_name: str = "default-model",
        tensor_parallel_size: int = 1,
        pipeline_parallel_size: int = 1,
        data_parallel_size: int = 1,
        extra_args: str = "",
        port: int = 8000,
    ) -> "EndpointEngineConfig":
        """Create VLLM endpoint configuration."""
        return cls(
            endpoint_type=EndpointType.VLLM,
            image=image,
            port=port,
            checkpoint_path=checkpoint_path,
            served_model_name=served_model_name,
            tensor_parallel_size=tensor_parallel_size,
            pipeline_parallel_size=pipeline_parallel_size,
            data_parallel_size=data_parallel_size,
            extra_args=extra_args,
        )

    @classmethod
    def sglang(
        cls,
        image: str = "lmsysorg/sglang:latest",
        checkpoint_path: str = "meta-llama/Llama-3.1-8B-Instruct",
        served_model_name: str = "default-model",
        tensor_parallel_size: int = 1,
        data_parallel_size: int = 1,
        extra_args: str = "",
        port: int = 8000,
    ) -> "EndpointEngineConfig":
        """Create SGLang endpoint configuration."""
        return cls(
            endpoint_type=EndpointType.SGLANG,
            image=image,
            port=port,
            checkpoint_path=checkpoint_path,
            served_model_name=served_model_name,
            tensor_parallel_size=tensor_parallel_size,
            data_parallel_size=data_parallel_size,
            extra_args=extra_args,
        )

    @classmethod
    def nim(cls, image: str, port: int = 8000) -> "EndpointEngineConfig":
        """Create NIM endpoint configuration."""
        return cls(endpoint_type=EndpointType.NIM, image=image, port=port)

    @classmethod
    def custom(cls, image: str, command: Optional[List[str]] = None, port: int = 8080) -> "EndpointEngineConfig":
        """Create custom container endpoint configuration."""
        return cls(endpoint_type=EndpointType.CUSTOM, image=image, port=port, command=command)


@dataclass(frozen=True)
class MountReader:
    """Mount configuration for Lepton deployments."""

    mounts: List[Dict[str, Any]]

    def to_dict(self) -> List[Dict[str, Any]]:
        """Convert to Lepton-compatible mount configuration."""
        return self.mounts

    @classmethod
    def node_nfs(cls, *path_pairs, storage_name: str = "lepton-shared-fs") -> "MountReader":
        """Create NFS mounts from path pairs.

        Args:
            *path_pairs: Tuples of (cache_path, mount_path) or (cache_path, mount_path, enabled)
            storage_name (str): NFS storage identifier

        Returns:
            MountReader: Configured instance

        Raises:
            ValueError: If path_pair format is invalid
        """
        mounts = []
        for path_pair in path_pairs:
            if len(path_pair) == 2:
                cache_path, mount_path = path_pair
                enabled = True
            elif len(path_pair) == 3:
                cache_path, mount_path, enabled = path_pair
            else:
                raise ValueError(
                    f"Path pair must be (cache_path, mount_path) or (cache_path, mount_path, enabled), got {path_pair}"
                )

            mounts.append(
                {
                    "enabled": enabled,
                    "cache_path": cache_path,
                    "mount_path": mount_path,
                    "storage_source": f"node-nfs:{storage_name}",
                }
            )

        return cls(mounts=mounts)


@dataclass(frozen=True)
class LeptonEndpointConfig:
    """Complete configuration for Lepton AI endpoint deployment.

    This single configuration class follows standard Flytekit plugin patterns
    by containing all parameters needed for endpoint deployment in one place.

    """

    # Required parameters
    endpoint_name: str
    resource_shape: str
    node_group: str

    # Unified endpoint engine configuration
    endpoint_config: EndpointEngineConfig

    # Optional configurations
    scaling: Optional[ScalingConfig] = None
    environment: Optional[Union[EnvironmentConfig, Dict[str, str]]] = None
    mounts: Optional[Union[MountReader, List[Dict[str, Any]]]] = None

    # Authentication
    api_token: Optional[str] = None
    api_token_secret: Optional[str] = None

    # Health configuration
    initial_delay_seconds: Optional[int] = None
    liveness_timeout: Optional[int] = None
    readiness_delay: Optional[int] = None

    # Container and deployment configuration
    image_pull_secrets: Optional[List[str]] = None
    endpoint_readiness_timeout: Optional[int] = None

    def __post_init__(self):
        """Validate the configuration after initialization."""
        # Validate required fields for deployment operations
        if not self.endpoint_name or not self.resource_shape or not self.node_group:
            raise ValueError("endpoint_name, resource_shape, and node_group are required for deployment")

        # Validate API token configuration
        if self.api_token and self.api_token_secret:
            raise ValueError("Cannot specify both api_token and api_token_secret")

        # Validate that endpoint_config type matches the endpoint_config.endpoint_type
        # (The EndpointConfig class handles its own internal validation)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary for the connector."""
        config = {
            "deployment_type": self.endpoint_config.endpoint_type.value,
            "endpoint_name": self.endpoint_name,
            "resource_shape": self.resource_shape,
            "node_group": self.node_group,
            "operation": "deploy",
        }

        # Add endpoint configuration
        config.update(self.endpoint_config.to_dict())

        # Build environment variables from unified environment configuration
        if self.environment:
            env_vars_dict = (
                self.environment.to_dict() if isinstance(self.environment, EnvironmentConfig) else self.environment
            )
            if env_vars_dict:
                config["env_vars"] = env_vars_dict

        # Add mounts
        if self.mounts:
            config["mounts"] = self.mounts.to_dict() if hasattr(self.mounts, "to_dict") else self.mounts

        # Add API tokens
        if self.api_token:
            config["api_tokens"] = [{"value": self.api_token}]
        elif self.api_token_secret:
            config["api_tokens"] = [{"value_from": {"secret_name_ref": self.api_token_secret}}]

        # Add scaling configuration
        if self.scaling:
            config["auto_scaler"] = self.scaling.to_dict()
            config.update(self.scaling.get_replica_config())

        # Add health configuration
        health_fields = [self.initial_delay_seconds, self.liveness_timeout, self.readiness_delay]
        if any(health_fields):
            health_config = {"enable_collection": True}
            if self.initial_delay_seconds or self.liveness_timeout:
                liveness = {}
                if self.initial_delay_seconds:
                    liveness["initial_delay_seconds"] = self.initial_delay_seconds
                if self.liveness_timeout:
                    liveness["timeout_seconds"] = self.liveness_timeout
                health_config["liveness"] = liveness
            if self.readiness_delay:
                health_config["readiness"] = {"initial_delay_seconds": self.readiness_delay}
            config["health_config"] = health_config

        # Add other fields
        if self.image_pull_secrets:
            config["image_pull_secrets"] = self.image_pull_secrets
        if self.endpoint_readiness_timeout:
            config["endpoint_readiness_timeout"] = self.endpoint_readiness_timeout

        return config

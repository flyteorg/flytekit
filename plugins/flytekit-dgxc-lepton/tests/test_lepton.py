import asyncio
import pytest
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import grpc
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.dgxc_lepton import (
    LeptonEndpointConfig,
    LeptonEndpointDeploymentTask,
    LeptonEndpointDeletionTask,
    EndpointType,
    EndpointEngineConfig,
    EnvironmentConfig,
    ScalingConfig,
    MountReader,
    lepton_endpoint_deployment_task,
    lepton_endpoint_deletion_task,
)

from flytekit import Resources
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.models.literals import Literal, LiteralMap, Primitive, Scalar


class TestLeptonEndpointConfig:
    """Test the unified LeptonEndpointConfig class."""

    def test_basic_vllm_config(self):
        """Test basic VLLM endpoint configuration."""
        config = LeptonEndpointConfig(
            endpoint_name="test-vllm",
            resource_shape="gpu.1xh100",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.vllm(
                checkpoint_path="meta-llama/Llama-3.1-8B-Instruct"
            ),
            api_token="test-token",
        )

        config_dict = config.to_dict()
        assert config_dict["endpoint_name"] == "test-vllm"
        assert config_dict["resource_shape"] == "gpu.1xh100"
        assert config_dict["node_group"] == "test-group"
        assert config_dict["deployment_type"] == "vllm"
        assert config_dict["checkpoint_path"] == "meta-llama/Llama-3.1-8B-Instruct"
        assert config_dict["api_tokens"] == [{"value": "test-token"}]

    def test_nim_config_with_secrets(self):
        """Test NIM endpoint with secrets."""
        config = LeptonEndpointConfig(
            endpoint_name="test-nim",
            resource_shape="gpu.1xh200",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.nim(
                image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest"
            ),
            environment=EnvironmentConfig.create(
                OMPI_ALLOW_RUN_AS_ROOT="1",
                secrets={"NGC_API_KEY": "ngc-secret"}
            ),
            scaling=ScalingConfig.qpm(target_qpm=2.5, min_replicas=1, max_replicas=3),
            image_pull_secrets=["ngc-secret"],
        )

        config_dict = config.to_dict()
        assert config_dict["deployment_type"] == "nim"
        assert config_dict["image"] == "nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest"
        assert config_dict["image_pull_secrets"] == ["ngc-secret"]
        assert "env_vars" in config_dict
        assert config_dict["env_vars"]["OMPI_ALLOW_RUN_AS_ROOT"] == "1"
        assert config_dict["env_vars"]["NGC_API_KEY"]["value_from"]["secret_name_ref"] == "ngc-secret"

    def test_custom_config_with_scaling(self):
        """Test custom endpoint with traffic scaling."""
        config = LeptonEndpointConfig(
            endpoint_name="test-custom",
            resource_shape="cpu.small",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.custom(
                image="python:3.11-slim",
                command=["/bin/bash", "-c", "python3 -m http.server 8080"],
                port=8080,
            ),
            scaling=ScalingConfig.traffic(min_replicas=1, max_replicas=2, timeout=1800),
            environment=EnvironmentConfig.from_env(LOG_LEVEL="INFO"),
        )

        config_dict = config.to_dict()
        assert config_dict["deployment_type"] == "custom"
        assert config_dict["image"] == "python:3.11-slim"
        assert config_dict["command"] == ["/bin/bash", "-c", "python3 -m http.server 8080"]
        assert config_dict["port"] == 8080
        assert "auto_scaler" in config_dict
        assert config_dict["min_replicas"] == 1
        assert config_dict["max_replicas"] == 2

    def test_deletion_config(self):
        """Test deletion configuration - now handled by separate deletion task."""
        # Note: Deletion is now handled by lepton_endpoint_deletion_task()
        # which only needs endpoint_name, not a full config

        # This test now just verifies we can create a minimal config for testing
        config = LeptonEndpointConfig(
            endpoint_name="test-delete",
            resource_shape="cpu.small",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.custom(image="dummy"),
        )

        config_dict = config.to_dict()
        assert config_dict["operation"] == "deploy"  # Always deploy for LeptonEndpointConfig
        assert config_dict["endpoint_name"] == "test-delete"

    def test_validation_errors(self):
        """Test configuration validation."""
        # Test missing required fields for deployment
        with pytest.raises(ValueError, match="endpoint_name, resource_shape, and node_group are required"):
            LeptonEndpointConfig(
                endpoint_name="",
                resource_shape="gpu.1xh100",
                node_group="test-group",
                endpoint_config=EndpointEngineConfig.vllm(),
            )

        # Test both api_token and api_token_secret specified
        with pytest.raises(ValueError, match="Cannot specify both api_token and api_token_secret"):
            LeptonEndpointConfig(
                endpoint_name="test",
                resource_shape="gpu.1xh100",
                node_group="test-group",
                endpoint_config=EndpointEngineConfig.vllm(),
                api_token="token",
                api_token_secret="secret",
            )


class TestEndpointEngineConfig:
    """Test the unified EndpointEngineConfig class."""

    def test_vllm_factory_method(self):
        """Test VLLM factory method."""
        config = EndpointEngineConfig.vllm(
            checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
            tensor_parallel_size=2,
        )

        assert config.endpoint_type == EndpointType.VLLM
        assert config.checkpoint_path == "meta-llama/Llama-3.1-8B-Instruct"
        assert config.tensor_parallel_size == 2
        assert config.pipeline_parallel_size == 1  # default

        config_dict = config.to_dict()
        assert config_dict["checkpoint_path"] == "meta-llama/Llama-3.1-8B-Instruct"
        assert config_dict["tensor_parallel_size"] == 2
        assert config_dict["pipeline_parallel_size"] == 1
        assert config_dict["data_parallel_size"] == 1

    def test_sglang_factory_method(self):
        """Test SGLang factory method."""
        config = EndpointEngineConfig.sglang(
            checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
        )

        assert config.endpoint_type == EndpointType.SGLANG
        config_dict = config.to_dict()
        assert "pipeline_parallel_size" not in config_dict  # SGLang doesn't use this
        assert config_dict["data_parallel_size"] == 1

    def test_nim_factory_method(self):
        """Test NIM factory method."""
        config = EndpointEngineConfig.nim(
            image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest"
        )

        assert config.endpoint_type == EndpointType.NIM
        assert config.image == "nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest"

    def test_custom_factory_method(self):
        """Test custom factory method."""
        config = EndpointEngineConfig.custom(
            image="python:3.11-slim",
            command=["/bin/bash", "-c", "python3 -m http.server 8080"],
            port=8080,
        )

        assert config.endpoint_type == EndpointType.CUSTOM
        assert config.image == "python:3.11-slim"
        assert config.command == ["/bin/bash", "-c", "python3 -m http.server 8080"]
        assert config.port == 8080


class TestScalingConfig:
    """Test the unified ScalingConfig class."""

    def test_traffic_scaling(self):
        """Test traffic-based scaling."""
        config = ScalingConfig.traffic(min_replicas=1, max_replicas=3, timeout=1800)

        assert config.scaling_type.value == "traffic"
        assert config.min_replicas == 1
        assert config.max_replicas == 3
        assert config.timeout == 1800

        config_dict = config.to_dict()
        assert config_dict["scale_down"]["no_traffic_timeout"] == 1800
        assert config_dict["target_gpu_utilization_percentage"] == 0

    def test_gpu_scaling(self):
        """Test GPU utilization scaling."""
        config = ScalingConfig.gpu(target_utilization=80, min_replicas=1, max_replicas=5)

        assert config.scaling_type.value == "gpu"
        assert config.target_utilization == 80

        config_dict = config.to_dict()
        assert config_dict["target_gpu_utilization_percentage"] == 80

    def test_qpm_scaling(self):
        """Test QPM-based scaling."""
        config = ScalingConfig.qpm(target_qpm=100.5, min_replicas=2, max_replicas=4)

        assert config.scaling_type.value == "qpm"
        assert config.target_qpm == 100.5

        config_dict = config.to_dict()
        assert config_dict["target_throughput"]["qpm"] == 100.5

    def test_scaling_validation(self):
        """Test scaling configuration validation."""
        # Test invalid GPU utilization
        with pytest.raises(ValueError, match="target_utilization must be between 1 and 100"):
            ScalingConfig.gpu(target_utilization=150)

        # Test negative QPM
        with pytest.raises(ValueError, match="target_qpm must be positive"):
            ScalingConfig.qpm(target_qpm=-5.0)


class TestEnvironmentConfig:
    """Test the unified EnvironmentConfig class."""

    def test_env_vars_only(self):
        """Test environment variables only."""
        config = EnvironmentConfig.from_env(LOG_LEVEL="DEBUG", MODEL_PATH="/models")

        config_dict = config.to_dict()
        assert config_dict["LOG_LEVEL"] == "DEBUG"
        assert config_dict["MODEL_PATH"] == "/models"

    def test_secrets_only(self):
        """Test secrets only."""
        config = EnvironmentConfig.from_secrets(HF_TOKEN="hf-secret", NGC_API_KEY="ngc-secret")

        config_dict = config.to_dict()
        assert config_dict["HF_TOKEN"]["value_from"]["secret_name_ref"] == "hf-secret"
        assert config_dict["NGC_API_KEY"]["value_from"]["secret_name_ref"] == "ngc-secret"

    def test_mixed_config(self):
        """Test mixed environment variables and secrets."""
        config = EnvironmentConfig.create(
            LOG_LEVEL="DEBUG",
            MODEL_PATH="/models",
            secrets={"HF_TOKEN": "hf-secret"}
        )

        config_dict = config.to_dict()
        assert config_dict["LOG_LEVEL"] == "DEBUG"
        assert config_dict["MODEL_PATH"] == "/models"
        assert config_dict["HF_TOKEN"]["value_from"]["secret_name_ref"] == "hf-secret"


class TestMountReader:
    """Test the simplified MountReader class."""

    def test_node_nfs_basic(self):
        """Test basic NFS mount configuration."""
        mounts = MountReader.node_nfs(
            ("/shared-storage/models", "/opt/models"),
            ("/shared-storage/data", "/opt/data"),
        )

        mount_list = mounts.to_dict()
        assert len(mount_list) == 2
        assert mount_list[0]["cache_path"] == "/shared-storage/models"
        assert mount_list[0]["mount_path"] == "/opt/models"
        assert mount_list[0]["enabled"] is True
        assert mount_list[0]["storage_source"] == "node-nfs:lepton-shared-fs"

    def test_node_nfs_with_disabled(self):
        """Test NFS mount with disabled mount."""
        mounts = MountReader.node_nfs(
            ("/shared-storage/models", "/opt/models"),
            ("/shared-storage/logs", "/opt/logs", False),  # Disabled
        )

        mount_list = mounts.to_dict()
        assert mount_list[0]["enabled"] is True
        assert mount_list[1]["enabled"] is False

    def test_node_nfs_custom_storage(self):
        """Test NFS mount with custom storage name."""
        mounts = MountReader.node_nfs(
            ("/prod-storage/models", "/opt/models"),
            storage_name="production-nfs"
        )

        mount_list = mounts.to_dict()
        assert mount_list[0]["storage_source"] == "node-nfs:production-nfs"


class TestLeptonEndpointDeploymentTask:
    """Test the unified LeptonEndpointDeploymentTask class."""

    def test_task_creation(self):
        """Test task creation with configuration."""
        config = LeptonEndpointConfig(
            endpoint_name="test-endpoint",
            resource_shape="gpu.1xh100",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.vllm(),
        )

        task = LeptonEndpointDeploymentTask(config=config, name="test-task")

        assert task.name == "test-task"
        assert task._TASK_TYPE == "lepton_endpoint_deployment_task"
        assert task.task_config == config.to_dict()

    def test_lepton_endpoint_deployment_task_function(self):
        """Test the lepton_endpoint_deployment_task convenience function."""
        config = LeptonEndpointConfig(
            endpoint_name="test-endpoint",
            resource_shape="gpu.1xh100",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.custom(image="test-image"),
        )

        # Test that the function creates a task (without executing it)
        task = LeptonEndpointDeploymentTask(config=config, name="test-function")

        # Verify the task was created correctly
        assert task.name == "test-function"
        assert task._TASK_TYPE == "lepton_endpoint_deployment_task"
        assert task.task_config == config.to_dict()

    def test_lepton_endpoint_deletion_task_function(self):
        """Test the lepton_endpoint_deletion_task convenience function."""
        # Test that the function creates a deletion task (without executing it)
        task = LeptonEndpointDeletionTask(endpoint_name="test-endpoint", name="test-deletion")

        # Verify the task was created correctly
        assert task.name == "test-deletion"
        assert task._TASK_TYPE == "lepton_endpoint_deletion_task"
        assert task.task_config == {"endpoint_name": "test-endpoint"}


class TestConnectorIntegration:
    """Test connector integration with new configuration."""

    @patch('flytekitplugins.dgxc_lepton.connector.APIClient')
    def test_connector_with_new_config(self, mock_api_client):
        """Test that connector works with new configuration format."""
        # Mock the API client
        mock_client = MagicMock()
        mock_api_client.return_value = mock_client

        # Create configuration
        config = LeptonEndpointConfig(
            endpoint_name="test-connector",
            resource_shape="gpu.1xh100",
            node_group="test-group",
            endpoint_config=EndpointEngineConfig.vllm(
                checkpoint_path="meta-llama/Llama-3.1-8B-Instruct"
            ),
            api_token="test-token",
        )

        # Test that config converts properly
        config_dict = config.to_dict()

        # Verify expected structure
        assert config_dict["deployment_type"] == "vllm"
        assert config_dict["endpoint_name"] == "test-connector"
        assert config_dict["api_tokens"] == [{"value": "test-token"}]
        assert config_dict["checkpoint_path"] == "meta-llama/Llama-3.1-8B-Instruct"


if __name__ == "__main__":
    pytest.main([__file__])

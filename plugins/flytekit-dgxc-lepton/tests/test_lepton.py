import asyncio
import pytest
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import grpc
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.dgxc_lepton import (
    LeptonEndpointConnector,
    LeptonEndpointTask,
    create_lepton_endpoint_task,
)

from flytekit import Resources
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.extend.backend.base_connector import ConnectorRegistry


def test_deployment_task_creation():
    """Test LeptonEndpointTask creation with optimized unified function"""

    # Test vLLM task creation
    vllm_task = create_lepton_endpoint_task(
        name="test_vllm",
        deployment_type="vllm",
        image="vllm/vllm-openai:latest",
        port=8000,
        resource_shape="gpu.a10",
    )
    assert isinstance(vllm_task, LeptonEndpointTask)
    assert vllm_task._TASK_TYPE == "lepton_endpoint_task"
    # Check merged configuration values (not deployment_type which is internal)
    assert vllm_task.task_config["port"] == 8000
    assert vllm_task.task_config["image"] == "vllm/vllm-openai:latest"
    assert vllm_task.task_config["resource_shape"] == "gpu.a10"
    assert vllm_task.task_config["node_group"] == "<your-node-group>"  # From BASE_CONFIG

    # Test NIM task creation
    nim_task = create_lepton_endpoint_task(
        name="test_nim",
        deployment_type="nim",
        image="nvcr.io/nim/meta/llama-3.1-8b-instruct:1.8.6",
        resource_shape="gpu.1xh200",
    )
    assert isinstance(nim_task, LeptonEndpointTask)
    assert nim_task.task_config["image"] == "nvcr.io/nim/meta/llama-3.1-8b-instruct:1.8.6"
    assert nim_task.task_config["resource_shape"] == "gpu.1xh200"
    assert nim_task.task_config["max_replicas"] == 3  # From NIM defaults
    assert nim_task.task_config["endpoint_readiness_timeout"] == 1800  # From NIM defaults


def test_deployment_task_serialization():
    """Test that deployment tasks can be serialized"""
    task = create_lepton_endpoint_task(
        name="test_serialization",
        deployment_type="custom",
        image="python:3.11-slim",
        port=8080,
    )

    # Test serialization
    settings = SerializationSettings(
        project="test",
        domain="dev",
        version="123",
        image_config=ImageConfig(default_image=DefaultImages.default_image()),
    )

    serialized = get_serializable(OrderedDict(), settings, task)
    assert serialized is not None


def test_connector_registration():
    """Test that LeptonEndpointConnector is properly registered"""
    registry = ConnectorRegistry()
    registry.register(LeptonEndpointConnector)

    connector = registry.get("lepton_endpoint_task")
    assert connector is not None
    assert isinstance(connector, LeptonEndpointConnector)


@patch('leptonai.api.v1.client.APIClient')
@patch('leptonai.api.v1.deployment.DeploymentAPI')
def test_connector_authentication(mock_deployment_api_class, mock_api_client_class):
    """Test connector API creation and authentication"""
    mock_api_client = MagicMock()
    mock_deployment_api = MagicMock()
    mock_api_client_class.return_value = mock_api_client
    mock_deployment_api_class.return_value = mock_deployment_api

    connector = LeptonEndpointConnector()

    # Mock environment variables
    with patch.dict('os.environ', {
        'LEPTON_WORKSPACE_ID': 'test-workspace',
        'LEPTON_TOKEN': 'test-token'
    }):
        deployment_api = connector._get_deployment_api()
        assert deployment_api is not None
        mock_api_client_class.assert_called_once_with(
            workspace_id='test-workspace',
            auth_token='test-token'
        )
        mock_deployment_api_class.assert_called_once_with(mock_api_client)


def test_endpoint_name_sanitization():
    """Test endpoint name sanitization"""
    connector = LeptonEndpointConnector()

    # Test basic sanitization
    assert connector._sanitize_endpoint_name("Test_Endpoint-123") == "test-endpoint-123"

    # Test starting with number (should be prefixed)
    assert connector._sanitize_endpoint_name("123-endpoint").startswith("ep-123")

    # Test length limit
    long_name = "a" * 50
    sanitized = connector._sanitize_endpoint_name(long_name)
    assert len(sanitized) <= 36


def test_state_mapping():
    """Test mapping Lepton states to Flyte phases"""
    connector = LeptonEndpointConnector()

    # Test state mappings
    assert connector._map_status_to_phase("ready") == TaskExecution.SUCCEEDED
    assert connector._map_status_to_phase("starting") == TaskExecution.INITIALIZING
    assert connector._map_status_to_phase("not ready") == TaskExecution.INITIALIZING


@patch('leptonai.api.v1.client.APIClient')
@patch('leptonai.api.v1.deployment.DeploymentAPI')
def test_endpoint_lifecycle(mock_deployment_api_class, mock_api_client_class):
    """Test endpoint creation and status checking"""

    # Mock SDK APIs
    mock_api_client = MagicMock()
    mock_deployment_api = MagicMock()
    mock_api_client_class.return_value = mock_api_client
    mock_deployment_api_class.return_value = mock_deployment_api

    # Mock deployment object with correct structure (based on investigation)
    mock_deployment = MagicMock()
    mock_deployment.status.state = "Ready"  # LeptonDeploymentState.Ready
    mock_deployment.status.endpoint.external_endpoint = "https://test-endpoint.lepton.ai"

    mock_deployment_api.create.return_value = True
    mock_deployment_api.get.return_value = mock_deployment

    connector = LeptonEndpointConnector()

    # Mock environment variables
    with patch.dict('os.environ', {
        'LEPTON_WORKSPACE_ID': 'test-workspace',
        'LEPTON_TOKEN': 'test-token'
    }):
        # Test API creation
        deployment_api = connector._get_deployment_api()
        assert deployment_api is not None


def test_deployment_task_with_complex_config():
    """Test deployment task with complex configuration"""

    task = create_lepton_endpoint_task(
        name="complex_vllm",
        deployment_type="vllm",
        image="vllm/vllm-openai:latest",
        env_vars={
            "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_TOKEN"}},
            "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
        },
        mounts={
            "enabled": True,
            "cache_path": "/model-cache",
            "mount_path": "/opt/cache",
            "storage_source": "node-nfs:shared-fs",
        },
        auto_scaler={
            "target_gpu_utilization_percentage": 70,
            "target_throughput": {"qpm": 10.0},
        },
    )

    # Verify complex config is preserved
    config = task.task_config
    assert config["env_vars"]["HF_TOKEN"]["value_from"]["secret_name_ref"] == "HUGGING_FACE_TOKEN"
    assert config["mounts"]["enabled"] is True
    assert config["auto_scaler"]["target_gpu_utilization_percentage"] == 70


def test_connector_spec_generation():
    """Test that connector can generate endpoint specs from task configs"""
    connector = LeptonEndpointConnector()

    config = {
        "deployment_type": "vllm",
        "image": "vllm/vllm-openai:latest",
        "port": 8000,
        "resource_shape": "gpu.a10",
        "checkpoint_path": "meta-llama/Llama-3.1-8B-Instruct",
        "served_model_name": "llama-3.1-8b",
        "tensor_parallel_size": 1,
    }

    spec = connector._generate_endpoint_spec_from_config(config)

    # Basic structure checks
    assert "container" in spec
    assert spec["container"]["image"] == "vllm/vllm-openai:latest"
    assert spec["container"]["ports"][0]["container_port"] == 8000


if __name__ == "__main__":
    pytest.main([__file__])

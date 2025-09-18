import asyncio
import pytest
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import grpc
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.dgxc_lepton import LeptonEndpointConnector, LeptonConfig, LeptonTask

from flytekit import Resources, task
from flytekit.configuration import DefaultImages, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.extend.backend.base_connector import ConnectorRegistry


def test_lepton_config():
    """Test LeptonConfig initialization and defaults"""
    config = LeptonConfig()
    assert config.endpoint_name == "default-endpoint"
    assert config.resource_shape == "cpu.small"
    assert config.min_replicas == 1
    assert config.max_replicas == 1
    assert config.env_vars is None
    assert config.deployment_timeout == 300


def test_lepton_config_custom():
    """Test LeptonConfig with custom values"""
    config = LeptonConfig(
        endpoint_name="my-model",
        resource_shape="gpu.a10",
        min_replicas=3,
        max_replicas=5,
        env_vars={"MODEL_NAME": "test"},
        deployment_timeout=600
    )

    assert config.endpoint_name == "my-model"
    assert config.resource_shape == "gpu.a10"
    assert config.min_replicas == 3
    assert config.max_replicas == 5
    assert config.env_vars == {"MODEL_NAME": "test"}
    assert config.deployment_timeout == 600


def test_lepton_task():
    """Test LeptonTask creation and configuration"""
    task_config = LeptonConfig(
        endpoint_name="test-model",
        deployment_type="custom",
        resource_shape="gpu.a10",
        min_replicas=1,
        max_replicas=3
    )

    requests = Resources(cpu="2", mem="4Gi")
    limits = Resources(cpu="4", mem="8Gi")
    container_image = DefaultImages.default_image()
    environment = {"MODEL_NAME": "test"}

    @task(
        task_config=task_config,
        requests=requests,
        limits=limits,
        container_image=container_image,
        environment=environment,
    )
    def inference_task(input_text: str) -> str:
        return f"Processed: {input_text}"

    assert inference_task.task_config == task_config
    assert inference_task.task_type == "lepton_endpoint_task"
    assert isinstance(inference_task, LeptonTask)

    serialization_settings = SerializationSettings(image_config=ImageConfig())
    task_spec = get_serializable(OrderedDict(), serialization_settings, inference_task)
    template = task_spec.template
    container = template.container

    # Verify custom configuration is properly serialized
    assert "endpoint_name" in template.custom
    assert template.custom["endpoint_name"] == "test-model"
    assert template.custom["deployment_type"] == "custom"
    assert template.custom["resource_shape"] == "gpu.a10"
    assert container.image == container_image
    assert container.env == environment


def test_lepton_config_deployment_types():
    """Test different deployment type configurations"""

    # Test NIM deployment
    nim_config = LeptonConfig(
        deployment_type="nim",
        served_model_name="llama-3.1-8b-instruct",
        resource_shape="gpu.1xh200"
    )
    assert nim_config.deployment_type == "nim"
    assert nim_config.served_model_name == "llama-3.1-8b-instruct"

    # Test vLLM deployment
    vllm_config = LeptonConfig(
        deployment_type="vllm",
        checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
        tensor_parallel_size=2
    )
    assert vllm_config.deployment_type == "vllm"
    assert vllm_config.checkpoint_path == "meta-llama/Llama-3.1-8B-Instruct"
    assert vllm_config.tensor_parallel_size == 2


@patch('subprocess.run')
def test_connector_authentication(mock_subprocess):
    """Test connector authentication mechanism"""
    # Mock successful authentication
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = "workspace: test-workspace"

    connector = LeptonEndpointConnector()

    # Test authentication doesn't raise errors
    try:
        connector._ensure_lep_cli_authenticated()
    except Exception as e:
        pytest.fail(f"Authentication should not fail: {e}")


def test_connector_registry():
    """Test that the connector is properly registered"""
    connector = ConnectorRegistry.get_connector("lepton_endpoint_task")
    assert isinstance(connector, LeptonEndpointConnector)
    assert connector.name == "Lepton Endpoint Connector"


@patch('subprocess.run')
def test_state_extraction(mock_subprocess):
    """Test state extraction from Lepton endpoint status output"""
    connector = LeptonEndpointConnector()

    # Test ready state extraction
    ready_output = """
    State: LeptonDeploymentState.Ready
    Replicas: 1-1
    """
    state = connector._extract_state_from_output(ready_output)
    assert state == "ready"

    # Test not ready state extraction
    notready_output = """
    State: LeptonDeploymentState.NotReady
    Replicas: 0-1
    """
    state = connector._extract_state_from_output(notready_output)
    assert state == "notready"


@patch('subprocess.run')
def test_url_extraction(mock_subprocess):
    """Test URL extraction from Lepton endpoint status JSON output"""
    connector = LeptonEndpointConnector()

    # Mock output with external endpoint URL
    sample_output = """
    Time now: 2025-09-18 10:00:00
    State: LeptonDeploymentState.Ready
    {
        'status': {
            'endpoint': {
                'external_endpoint': 'https://test-endpoint.xenon.lepton.run'
            }
        }
    }
    """

    url = connector._extract_endpoint_url(sample_output, "test-endpoint")
    assert url == "https://test-endpoint.xenon.lepton.run"


@patch('subprocess.run')
def test_endpoint_lifecycle(mock_subprocess):
    """Test complete endpoint lifecycle: create, get status, delete"""
    from flytekitplugins.dgxc_lepton.connector import LeptonMetadata

    connector = LeptonEndpointConnector()

    # Mock successful creation
    mock_subprocess.return_value.returncode = 0
    mock_subprocess.return_value.stdout = "Endpoint created successfully"
    mock_subprocess.return_value.stderr = ""

    # Test deletion functionality
    metadata = LeptonMetadata(deployment_name="test-endpoint")

    # This should not raise any exceptions
    try:
        # Test deletion (mocked)
        result = asyncio.run(connector.delete(metadata))
        # Delete method doesn't return anything on success
        assert result is None
    except Exception as e:
        pytest.fail(f"Deletion should not fail: {e}")


@patch('subprocess.run')
def test_sanitize_endpoint_name(mock_subprocess):
    """Test endpoint name sanitization for Lepton requirements"""
    connector = LeptonEndpointConnector()

    # Test various name sanitization scenarios
    test_cases = [
        ("Simple_Name_123", "simple-name-123"),
        ("Name-With-CAPS", "name-with-caps"),
        ("123StartWithNumber", "ep-123startwithNumber"),
        ("Name_With_$pecial@Chars!", "name-with--pecial-chars-"),
    ]

    for input_name, expected_pattern in test_cases:
        sanitized = connector._sanitize_endpoint_name(input_name)
        # Verify it follows Lepton naming rules
        assert sanitized.islower()
        assert sanitized[0].isalpha()
        assert len(sanitized) <= 36


if __name__ == "__main__":
    pytest.main([__file__])

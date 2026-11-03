"""
Example: Basic Inference using Task API

This example demonstrates the new task-based API for Lepton AI
endpoint lifecycle management, showing different workflow patterns.

"""

from flytekitplugins.dgxc_lepton import (
    EndpointEngineConfig,
    EnvironmentConfig,
    LeptonEndpointConfig,
    ScalingConfig,
    lepton_endpoint_deletion_task,
    lepton_endpoint_deployment_task,
)

from flytekit import workflow


@workflow
def basic_inference_workflow() -> str:
    """Simple workflow using the unified task API."""

    # Complete configuration in one place
    config = LeptonEndpointConfig(
        endpoint_name="basic-inference-endpoint",
        resource_shape="cpu.small",
        node_group="<your-node-group>",  # Replace with your actual node group name
        endpoint_config=EndpointEngineConfig.custom(
            image="python:3.11-slim",
            command=["/bin/bash", "-c", "python3 -m http.server 8080 --bind 0.0.0.0"],
            port=8080,
        ),
        api_token="BASIC_ENDPOINT_TOKEN",
        scaling=ScalingConfig.traffic(min_replicas=1, max_replicas=1, timeout=1800),
        environment=EnvironmentConfig.from_env(
            LOG_LEVEL="INFO",
            SERVER_MODE="production",
        ),
        endpoint_readiness_timeout=300,
    )

    return lepton_endpoint_deployment_task(config=config, task_name="basic-inference-endpoint-v3")


@workflow
def basic_inference_cleanup_workflow() -> str:
    """Cleanup workflow to delete the deployed endpoint."""

    # Much simpler deletion - just the endpoint name!
    return lepton_endpoint_deletion_task(
        endpoint_name="basic-inference-endpoint", task_name="basic-inference-cleanup-v2"
    )


if __name__ == "__main__":
    # Local execution examples

    # Example: Deploy endpoint
    result = basic_inference_workflow()
    print(f"Endpoint URL: {result}")

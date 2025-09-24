"""
Example: Basic Inference Endpoint with Lepton

This example demonstrates how to create a simple HTTP server endpoint
using the unified Lepton API.
"""

from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

from flytekit import workflow

# Create a basic HTTP server deployment task using unified API
basic_http_deployment = create_lepton_endpoint_task(
    deployment_type="custom",
    name="deploy_basic_http_server",
    image="python:3.11-slim",
    port=8080,
    resource_shape="cpu.small",
    command=["/bin/bash", "-c", "python3 -m http.server 8080 --bind 0.0.0.0"],
    api_tokens=[{"value": "BASIC_ENDPOINT_TOKEN"}],
    endpoint_readiness_timeout=300,
)


@workflow
def basic_inference_workflow() -> str:
    """
    Simple workflow that creates a Lepton inference endpoint.
    The endpoint will remain available after successful completion.
    """
    # Deploy endpoint - this returns the endpoint URL
    endpoint_url = basic_http_deployment(endpoint_name="basic-inference-endpoint", deployment_type="custom")
    return endpoint_url


if __name__ == "__main__":
    # Local execution example
    result = basic_inference_workflow()
    print(f"Endpoint URL: {result}")

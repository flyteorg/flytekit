"""
Example: Basic Inference Endpoint with Lepton

This example demonstrates how to create a simple HTTP server endpoint
using the Lepton plugin for Flytekit.
"""

from flytekitplugins.dgxc_lepton import LeptonConfig

from flytekit import task, workflow


@task(
    task_config=LeptonConfig(
        endpoint_name="basic-inference-endpoint",
        image="python:3.11-slim",
        deployment_type="custom",
        port=8080,
        command=[
            "/bin/bash",
            "-c",
            """
        python3 -c "
import http.server
import socketserver
import sys

print('Starting HTTP server on port 8080...')
handler = http.server.SimpleHTTPRequestHandler
httpd = socketserver.TCPServer(('', 8080), handler)
print('Server is ready to accept connections')
httpd.serve_forever()
        "
        """,
        ],
        resource_shape="cpu.small",
        min_replicas=1,
        max_replicas=1,
        node_group="<node-group-name>",  # Working node group
        api_tokens=[{"value": "UNIQUE_ENDPOINT_TOKEN"}],
        auto_scaler={
            "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
            "target_gpu_utilization_percentage": 0,
            "target_throughput": {"qpm": 2.5, "paths": [], "methods": []},
        },
    )
)
def inference_endpoint_task(input_text: str) -> str:
    """Deploy and run inference on a Lepton endpoint."""
    # This will return the endpoint URL when run remotely via our connector
    return f"Processed: {input_text}"


@workflow
def basic_inference_workflow(input_text: str = "Hello World") -> str:
    """
    Simple workflow that creates a Lepton inference endpoint.
    The endpoint will remain available after successful completion.
    """
    # Create Lepton endpoint - this returns the endpoint URL and name
    result = inference_endpoint_task(input_text=input_text)
    return result


if __name__ == "__main__":
    # Local execution example
    result = basic_inference_workflow(input_text="Test input")
    print(f"Workflow result: {result}")

    print("\nTo run remotely:")
    print(
        "pyflyte run -p flytesnacks -d development --remote examples/basic_inference.py basic_inference_workflow --input_text 'Hello'"
    )
    print("\nThis workflow will:")
    print("1. Create a basic HTTP server endpoint")
    print("2. Return the endpoint URL")
    print("3. Endpoint remains available for inference")
    print("4. Only uses Lepton connector tasks")

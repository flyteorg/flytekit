"""
Advanced Usage Examples for Flytekit DGXC Lepton Plugin

This file demonstrates various deployment patterns:
1. Basic Custom Container Deployment
2. NVIDIA NIM Deployment
3. vLLM Model Deployment

Each example shows different configuration options and use cases.
"""

from flytekitplugins.dgxc_lepton import LeptonConfig

from flytekit import task, workflow


# Example 1: Basic Custom Container Deployment
@task(
    task_config=LeptonConfig(
        endpoint_name="basic-http-server",
        image="python:3.11-slim",
        deployment_type="custom",
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
def basic_inference_task(input_text: str) -> str:
    """Deploy a basic HTTP server endpoint."""
    return f"Basic processing: {input_text}"


# Example 2: NVIDIA NIM Deployment
@task(
    task_config=LeptonConfig(
        endpoint_name="nim-llama-3-1-8b-instruct",
        image="nvcr.io/nim/meta/llama-3.1-8b-instruct:1.8.6",
        deployment_type="nim",
        port=8000,
        served_model_name="meta/llama-3.1-8b-instruct",
        resource_shape="gpu.1xh200",
        min_replicas=1,
        max_replicas=3,
        node_group="<node-group-name>",  # Working node group
        # Environment variables with secret references for NIM
        env_vars={
            "OMPI_ALLOW_RUN_AS_ROOT": "1",
            "NGC_API_KEY": {"value_from": {"secret_name_ref": "NGC_API_KEY"}},
            "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_HUB_TOKEN_read"}},
        },
        # API tokens for endpoint access
        api_tokens=[{"value": "NIM_ENDPOINT_TOKEN"}],
        # Auto-scaling configuration for production workloads
        auto_scaler={
            "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
            "target_gpu_utilization_percentage": 0,
            "target_throughput": {"qpm": 2.5, "paths": [], "methods": []},
        },
        # Storage mounts for model caching
        mounts={
            "enabled": True,
            "cache_path": "/cachepath",
            "mount_path": "/opt/nim/.cache",
            "storage_source": "node-nfs:lepton-shared-fs",
        },
        # Image pull secrets for NGC registry
        image_pull_secrets=["lepton-nvidia-ansjindal-2"],
        endpoint_readiness_timeout=1800,  # 30 minutes for large model loading
    )
)
def nim_inference_task(prompt: str) -> str:
    """Deploy and run inference on NVIDIA NIM endpoint."""
    return f"NIM inference result for: {prompt}"


# Example 3: vLLM Model Deployment
@task(
    task_config=LeptonConfig(
        endpoint_name="vllm-llama-3-1-8b-eval",
        image="vllm/vllm-openai:latest",
        deployment_type="vllm",
        port=8000,
        # Model configuration (required for vLLM)
        checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
        served_model_name="llama-3.1-8b-instruct",
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
        data_parallel_size=1,
        extra_args="--gpu-memory-utilization 0.95 --trust-remote-code --max-seq-len-to-capture 32768",
        # Resource configuration
        resource_shape="gpu.1xh200",
        min_replicas=1,
        max_replicas=2,
        node_group="<node-group-name>",  # Working node group
        # Environment variables
        env_vars={
            "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_HUB_TOKEN_read"}},
            "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
        },
        # Storage mounts for model weights
        mounts={
            "enabled": True,
            "cache_path": "/cachepath",
            "mount_path": "/opt/nim/.cache",
            "storage_source": "node-nfs:lepton-shared-fs",
        },
    )
)
def vllm_inference_task(prompt: str) -> str:
    """Deploy and run inference on vLLM endpoint."""
    return f"vLLM inference result for: {prompt}"


# Example Workflows
@workflow
def basic_inference_workflow(input_text: str = "Hello World") -> str:
    """Simple workflow with basic HTTP server endpoint."""
    result = basic_inference_task(input_text=input_text)
    return result


@workflow
def nim_inference_workflow(prompt: str = "What is artificial intelligence?") -> str:
    """Workflow using NVIDIA NIM for inference."""
    response = nim_inference_task(prompt=prompt)
    return response


@workflow
def vllm_inference_workflow(prompt: str = "Explain neural networks") -> str:
    """Workflow using vLLM for inference."""
    response = vllm_inference_task(prompt=prompt)
    return response


@workflow
def multi_model_comparison_workflow(prompt: str = "Compare machine learning frameworks") -> str:
    """Advanced workflow that compares responses from multiple models."""
    nim_response = nim_inference_task(prompt=prompt)
    vllm_response = vllm_inference_task(prompt=prompt)

    # Return a summary string since Flyte can't serialize dict with Promise objects
    return f"Completed comparison for prompt: '{prompt}' | NIM: {nim_response} | vLLM: {vllm_response}"


if __name__ == "__main__":
    # Local execution examples (will use mock responses)
    print("=== Basic Inference Workflow ===")
    basic_result = basic_inference_workflow(input_text="Test input")
    print(f"Basic result: {basic_result}")

    print("\n=== NIM Inference Workflow ===")
    nim_result = nim_inference_workflow(prompt="What is AI?")
    print(f"NIM result: {nim_result}")

    print("\n=== vLLM Inference Workflow ===")
    vllm_result = vllm_inference_workflow(prompt="Explain transformers")
    print(f"vLLM result: {vllm_result}")

    print("\n=== Multi-Model Comparison ===")
    comparison_result = multi_model_comparison_workflow(prompt="Compare PyTorch vs TensorFlow")
    print(f"Comparison result: {comparison_result}")

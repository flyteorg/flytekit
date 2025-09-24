"""
Example: vLLM Deployment with Lepton

This example demonstrates how to deploy a vLLM inference endpoint
using the new task-based Lepton API.
"""

from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

from flytekit import workflow

# Create a vLLM deployment task using unified API
vllm_deployment = create_lepton_endpoint_task(
    deployment_type="vllm",
    name="deploy_vllm_llama",
    image="vllm/vllm-openai:latest",
    port=8000,
    resource_shape="gpu.1xh200",
    min_replicas=1,
    max_replicas=2,
    tensor_parallel_size=1,
    pipeline_parallel_size=1,
    data_parallel_size=1,
    extra_args="--gpu-memory-utilization 0.95 --trust-remote-code",
    env_vars={
        "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_HUB_TOKEN_read"}},
        "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
    },
    api_tokens=[{"value": "VLLM_ENDPOINT_TOKEN"}],
    mounts={
        "enabled": True,
        "cache_path": "/shared-storage/model-cache/vllm-models",  # Replace with your cache path
        "mount_path": "/opt/nim/.cache",
        "storage_source": "node-nfs:lepton-shared-fs",
    },
)


@workflow
def vllm_inference_workflow() -> str:
    """Workflow that deploys and uses a vLLM inference endpoint."""
    endpoint_url = vllm_deployment(
        endpoint_name="vllm-llama-inference", resource_shape="gpu.1xh200", deployment_type="vllm"
    )
    return endpoint_url


if __name__ == "__main__":
    # Local execution example
    result = vllm_inference_workflow()
    print(f"vLLM endpoint URL: {result}")

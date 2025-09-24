"""
Example: vLLM Deployment with Lepton

This example demonstrates how to deploy a vLLM inference endpoint
using the new task-based Lepton API.
"""

from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

from flytekit import workflow

vllm_deployment = create_lepton_endpoint_task(
    deployment_type="vllm",
    name="deploy_vllm_llama_v8",
    image="vllm/vllm-openai:latest",
    port=8000,
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="llama-3.1-8b-instruct",
    tensor_parallel_size=1,
    pipeline_parallel_size=1,
    data_parallel_size=1,
    extra_args="--gpu-memory-utilization 0.95 --trust-remote-code",
    api_token="VLLM_ENDPOINT_TOKEN",
    secrets={"HF_TOKEN": "HUGGING_FACE_HUB_TOKEN_read"},
    scaling_mode="traffic",
    no_traffic_timeout=3600,
    scale_to_zero=True,
    # Direct environment variables (non-secrets)
    env_vars={
        "HF_HOME": "/opt/vllm/.cache",
        "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
    },
    # Mounts configuration
    mounts=[
        {
            "enabled": True,
            "cache_path": "/shared-storage/model-cache/vllm",
            "mount_path": "/opt/vllm/.cache",
            "storage_source": "node-nfs:lepton-shared-fs",
        }
    ],
)


@workflow
def vllm_inference_workflow() -> str:
    """Workflow that deploys and uses a vLLM inference endpoint."""
    endpoint_url = vllm_deployment(
        endpoint_name="vllm-llama-inference",
        resource_shape="gpu.1xh200",
        min_replicas=1,
        max_replicas=2,
        node_group="<your-node-group>",
    )
    return endpoint_url


if __name__ == "__main__":
    # Local execution example
    result = vllm_inference_workflow()
    print(f"vLLM endpoint URL: {result}")

"""
Example: vLLM Deployment with Lepton

This example demonstrates how to deploy a vLLM inference endpoint
using the Lepton plugin for Flytekit.
"""

from flytekitplugins.dgxc_lepton import LeptonConfig

from flytekit import task, workflow


@task(
    task_config=LeptonConfig(
        endpoint_name="vllm-llama-inference",
        image="vllm/vllm-openai:latest",
        deployment_type="vllm",
        port=8000,
        # Model configuration
        checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
        served_model_name="llama-3.1-8b-instruct",
        tensor_parallel_size=1,
        extra_args="--gpu-memory-utilization 0.95 --trust-remote-code",
        # Resource configuration
        resource_shape="gpu.1xh200",
        min_replicas=1,
        max_replicas=2,
        node_group="<node-group-name>",  # Working node group
        # Environment variables with secret references
        env_vars={
            "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_HUB_TOKEN_read"}},
        },
        # API tokens for endpoint access
        api_tokens=[{"value": "VLLM_ENDPOINT_TOKEN"}],
        # Storage mounts for model weights caching
        mounts={
            "enabled": True,
            "cache_path": "/cachepath",
            "mount_path": "/opt/nim/.cache",
            "storage_source": "node-nfs:lepton-shared-fs",
        },
    )
)
def vllm_inference_task(prompt: str) -> str:
    """Deploy and run inference on a vLLM endpoint."""
    return f"vLLM inference result for: {prompt}"


@workflow
def vllm_inference_workflow(prompt: str = "What is machine learning?") -> str:
    """Workflow that deploys and uses a vLLM inference endpoint."""
    response = vllm_inference_task(prompt=prompt)
    return response


if __name__ == "__main__":
    # Local execution example
    result = vllm_inference_workflow(prompt="Explain neural networks")
    print(f"vLLM response: {result}")

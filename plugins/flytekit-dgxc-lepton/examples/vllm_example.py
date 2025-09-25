"""
Example: vLLM Deployment using Task API with Parameterized Functions

This example demonstrates how the task API enables clean, reusable
vLLM deployments with parameterized functions for different environments.
"""

from typing import Optional

from flytekitplugins.dgxc_lepton import (
    EndpointEngineConfig,
    EnvironmentConfig,
    LeptonEndpointConfig,
    MountReader,
    ScalingConfig,
    lepton_endpoint_deployment_task,
)

from flytekit import workflow


def deploy_vllm_model(
    endpoint_name: str,
    resource_shape: str,
    node_group: str,
    mounts: MountReader,
    checkpoint_path: str = "meta-llama/Llama-3.1-8B-Instruct",
    served_model_name: Optional[str] = None,
    enable_secrets: bool = True,
) -> str:
    """Parameterized function to deploy vLLM models with configurable infrastructure.

    Args:
        endpoint_name (str): Name for the deployed endpoint
        resource_shape (str): GPU configuration (e.g., "gpu.1xh200", "gpu.1xa10")
        node_group (str): Kubernetes node group for deployment
        mounts (MountReader): Mount configuration for model storage
        checkpoint_path (str): HuggingFace model path
        served_model_name (Optional[str]): Name to serve the model as (defaults to endpoint_name)
        enable_secrets (bool): Whether to include HuggingFace token secret

    Returns:
        str: URL of the deployed vLLM endpoint
    """
    # Use endpoint_name as served_model_name if not provided
    if served_model_name is None:
        served_model_name = endpoint_name

    # Complete configuration in one place
    config = LeptonEndpointConfig(
        endpoint_name=endpoint_name,
        resource_shape=resource_shape,
        node_group=node_group,
        endpoint_config=EndpointEngineConfig.vllm(
            image="vllm/vllm-openai:latest",
            checkpoint_path=checkpoint_path,
            served_model_name=served_model_name,
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            data_parallel_size=1,
            extra_args="--gpu-memory-utilization 0.95 --trust-remote-code",
        ),
        api_token="VLLM_ENDPOINT_TOKEN",
        scaling=ScalingConfig.traffic(min_replicas=1, max_replicas=2, timeout=3600, scale_to_zero=True),
        environment=EnvironmentConfig.create(
            HF_HOME="/opt/vllm/.cache",
            VLLM_WORKER_MULTIPROC_METHOD="spawn",
            secrets={"HF_TOKEN": "HUGGING_FACE_HUB_TOKEN_read_updated"} if enable_secrets else None,
        ),
        mounts=mounts,
    )

    return lepton_endpoint_deployment_task(config=config, task_name="deploy_vllm_model_v3")


@workflow
def vllm_inference_workflow() -> str:
    """Deploy Llama 3.1 8B model using vLLM with production configuration."""

    # Configure production mounts
    model_cache_mounts = MountReader.node_nfs(
        ("<your-shared-storage>/vllm-models-cache", "/opt/vllm/.cache"),
    )

    return deploy_vllm_model(
        endpoint_name="vllm-llama-inference",
        resource_shape="gpu.1xh200",
        node_group="<your-gpu-node-group>",  # Replace with your actual GPU node group
        mounts=model_cache_mounts,
        checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
        served_model_name="llama-3.1-8b-instruct",
    )

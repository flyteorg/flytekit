"""
Example: NVIDIA NIM Nemotron Super Model Deployment

This example demonstrates deploying the latest Nemotron Super model
using NVIDIA NIM with comprehensive configuration similar to the reference spec.
"""

from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

from flytekit import workflow

# Create a comprehensive NIM deployment task for Nemotron Super model
nemotron_nim_deployment = create_lepton_endpoint_task(
    deployment_type="nim",
    name="deploy_nemotron_super",
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    port=8000,
    resource_shape="gpu.1xh200",
    min_replicas=1,
    max_replicas=3,
    env_vars={
        "OMPI_ALLOW_RUN_AS_ROOT": "1",
        "OMPI_ALLOW_RUN_AS_ROOT_CONFIRM": "1",
        "NGC_API_KEY": {
            "value_from": {
                "secret_name_ref": "NGC_API_KEY"  # Replace with your NGC secret
            }
        },
        "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_HUB_TOKEN_read"}},
        "SERVED_MODEL_NAME": "nvidia/llama-3_3-nemotron-super-49b-v1_5",
        "NIM_MODEL_NAME": "nvidia/llama-3_3-nemotron-super-49b-v1_5",
    },
    api_tokens=[{"value": "UNIQUE_ENDPOINT_TOKEN"}],
    auto_scaler={
        "scale_down": {
            "no_traffic_timeout": 0,
            "scale_from_zero": False,
        },
        "target_gpu_utilization_percentage": 0,
        "target_throughput": {"qpm": 2.5, "paths": [], "methods": []},
    },
    mounts={
        "enabled": True,
        "cache_path": "/shared-storage/model-cache/nim-models",  # Replace with your cache path
        "mount_path": "/opt/nim/.cache",
        "storage_source": "node-nfs:lepton-shared-fs",
    },
    image_pull_secrets=["<your-ngc-secret>"],  # Replace with your NGC pull secret
    # Custom health configuration for large model deployment
    health_config={
        "enable_collection": True,
        "liveness": {
            "initial_delay_seconds": 5000  # 5000 seconds for large model initialization
        },
    },
)


@workflow
def nemotron_super_workflow() -> str:
    """
    Deploy NVIDIA's latest Nemotron Super model for advanced AI reasoning.

    This workflow demonstrates the deployment of the most advanced NIM model
    with comprehensive configuration for production use.

    Returns:
        str: URL of the deployed Nemotron Super endpoint
    """
    endpoint_url = nemotron_nim_deployment(
        endpoint_name="nemotron-super-reasoning",
        # node_group inherited from task config
        deployment_type="nim",
    )

    return endpoint_url


if __name__ == "__main__":
    # Local execution examples
    print("=== Nemotron Super Reasoning Deployment ===")
    result1 = nemotron_super_workflow()
    print(f"Nemotron Super endpoint: {result1}")

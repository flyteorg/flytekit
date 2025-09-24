"""
Example: NVIDIA NIM Nemotron Super Model Deployment

This example demonstrates deploying the latest Nemotron Super model
using NVIDIA NIM with comprehensive configuration similar to the reference spec.
"""

from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

from flytekit import workflow

nemotron_nim_deployment = create_lepton_endpoint_task(
    deployment_type="nim",
    name="deploy_nemotron_super_v3",
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    port=8000,
    secrets={"NGC_API_KEY": "NGC_API_KEY", "HF_TOKEN": "HUGGING_FACE_HUB_TOKEN_read"},
    scaling_mode="qpm",
    target_qpm=2.5,
    initial_delay_seconds=5000,
    env_vars={
        "OMPI_ALLOW_RUN_AS_ROOT": "1",
        "OMPI_ALLOW_RUN_AS_ROOT_CONFIRM": "1",
        "SERVED_MODEL_NAME": "nvidia/llama-3_3-nemotron-super-49b-v1_5",
        "NIM_MODEL_NAME": "nvidia/llama-3_3-nemotron-super-49b-v1_5",
    },
    api_token="UNIQUE_ENDPOINT_TOKEN",
    image_pull_secrets=["<your-ngc-secret>"],
    mounts=[
        {
            "enabled": True,
            "cache_path": "/shared-storage/model-cache/nim",
            "mount_path": "/opt/nim/.cache",
            "storage_source": "node-nfs:lepton-shared-fs",
        }
    ],
)


@workflow
def nemotron_super_workflow() -> str:
    """Deploy NVIDIA's Nemotron Super model for advanced AI reasoning."""
    endpoint_url = nemotron_nim_deployment(
        endpoint_name="nemotron-super-reasoning",
        resource_shape="gpu.1xh200",
        min_replicas=1,
        max_replicas=3,
        node_group="<your-node-group>",
    )

    return endpoint_url


if __name__ == "__main__":
    # Local execution examples
    print("=== Nemotron Super Reasoning Deployment ===")
    result1 = nemotron_super_workflow()
    print(f"Nemotron Super endpoint: {result1}")

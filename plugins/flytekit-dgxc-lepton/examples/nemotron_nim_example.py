"""
Example: NVIDIA NIM using Task API

This example demonstrates deploying NVIDIA NIM models using the
task API for maximum flexibility and type safety.
"""

from flytekitplugins.dgxc_lepton import (
    EndpointEngineConfig,
    EnvironmentConfig,
    LeptonEndpointConfig,
    MountReader,
    ScalingConfig,
    lepton_endpoint_deployment_task,
)

from flytekit import workflow


@workflow
def nemotron_super_workflow() -> str:
    """Deploy Nemotron Super for advanced AI reasoning."""

    # Complete configuration in one place
    config = LeptonEndpointConfig(
        endpoint_name="nemotron-super-reasoning",
        resource_shape="gpu.1xh200",
        node_group="<your-gpu-node-group>",  # Replace with your actual GPU node group
        endpoint_config=EndpointEngineConfig.nim(
            image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
        ),
        api_token="UNIQUE_ENDPOINT_TOKEN",
        scaling=ScalingConfig.qpm(target_qpm=2.5, min_replicas=1, max_replicas=3),
        environment=EnvironmentConfig.create(
            OMPI_ALLOW_RUN_AS_ROOT="1",
            OMPI_ALLOW_RUN_AS_ROOT_CONFIRM="1",
            NIM_MODEL_NAME="nvidia/llama-3_3-nemotron-super-49b-v1_5",
            SERVED_MODEL_NAME="nvidia/llama-3_3-nemotron-super-49b-v1_5",
            secrets={
                "NGC_API_KEY": "<your-ngc-api-key-secret>",
                "HF_TOKEN": "HUGGING_FACE_HUB_TOKEN_read",
            },
        ),
        initial_delay_seconds=5000,
        image_pull_secrets=["<your-ngc-pull-secret>"],
        mounts=MountReader.node_nfs(
            ("<your-shared-storage>/nim-cache", "/opt/nim/.cache"),
            ("<your-shared-storage>/test-datasets", "/opt/nim/datasets"),
        ),
    )

    return lepton_endpoint_deployment_task(config=config, task_name="nemotron_super_workflow")

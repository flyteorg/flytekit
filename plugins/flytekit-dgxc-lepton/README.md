# Flytekit DGXC Lepton Plugin

A Flytekit plugin that enables seamless deployment and management of AI inference endpoints using Lepton AI infrastructure within Flyte workflows.

## Overview

This plugin provides:
- Automated deployment of AI inference endpoints via Lepton AI
- Seamless integration with Flyte's task execution model
- Support for multiple deployment types: NIM, vLLM, SGLang, and custom containers
- Advanced features including auto-scaling, secret management, and storage mounts

## Installation

```bash
pip install flytekitplugins-dgxc-lepton
```

## Prerequisites

### Backend Requirements
The Flyte backend requires:
- `leptonai` package installed in the connector environment
- Properly configured Lepton credentials

### Credentials Setup
Configure these secrets in your Flyte deployment:
- `lepton_workspace_id`: Your Lepton workspace identifier
- `lepton_token`: Your Lepton API authentication token
- `lepton_workspace_origin_url`: Your Lepton workspace origin URL

Users do not need local Lepton CLI installation - all operations are handled by the backend connector.

## Deployment

### Deploy the Connector Service

Deploy the dgxc-lepton connector as a dedicated connector service:

```bash
kubectl apply -f deployment/connector-deployment.yaml
```

### Required Secrets

Create the Lepton credentials secret:

```bash
kubectl create secret generic lepton-secrets \
  --from-literal=workspace_id=<your-workspace-id> \
  --from-literal=token=<your-api-token> \
  --from-literal=origin_url=<your-lepton-origin-url> \
  -n flyte

# Example for DGXC environment:
kubectl create secret generic lepton-secrets \
  --from-literal=workspace_id=xfre17eu \
  --from-literal=token=nvapi-xxxxx \
  --from-literal=origin_url=https://gateway.dgxc-lepton.nvidia.com \
  -n flyte
```

### Verification

After deployment, verify the connector is registered:

```bash
kubectl logs -n flyte deployment/lepton-connector | grep "Lepton Endpoint Connector"
```

You should see the connector listed in the supported task types.

## Quick Start

```python
from flytekit import workflow
from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

http_server = create_lepton_endpoint_task(
    deployment_type="custom",
    name="deploy_http_server",
    image="python:3.11-slim",
    command=["/bin/bash", "-c", "python3 -m http.server 8080 --bind 0.0.0.0"],
    api_token="my-server-token",
)

@workflow
def inference_workflow(endpoint_name: str = "my-endpoint") -> str:
    endpoint_url = http_server(
        endpoint_name=endpoint_name,
        resource_shape="cpu.small",
        min_replicas=1,
        max_replicas=1,
        node_group="<your-node-group>",
    )
    return endpoint_url
```

## Supported Deployment Types

The unified `create_lepton_endpoint_task()` function supports all deployment types with smart defaults:

### 1. Custom Containers
```python
custom_task = create_lepton_endpoint_task(
    deployment_type="custom",
    image="python:3.11-slim",
    port=8080,
    command=["/bin/bash", "-c", "python3 -m http.server 8080 --bind 0.0.0.0"],
    api_token="custom-server-token",
)
```

### 2. NVIDIA NIM
```python
nim_task = create_lepton_endpoint_task(
    deployment_type="nim",
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    secrets={"NGC_API_KEY": "ngc-secret", "HF_TOKEN": "hf-secret"},
    api_token="nim-endpoint-token",
    scaling_mode="traffic",
    scale_to_zero=True,
    initial_delay_seconds=5000,
)
```

### 3. vLLM
```python
vllm_task = create_lepton_endpoint_task(
    deployment_type="vllm",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="llama-3.1-8b",
    secrets={"HF_TOKEN": "hf-secret"},
    api_token="vllm-endpoint-token",
    tensor_parallel_size=2,
    extra_args="--gpu-memory-utilization 0.95 --trust-remote-code",
)
```

### 4. SGLang
```python
sglang_task = create_lepton_endpoint_task(
    deployment_type="sglang",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    secrets={"HF_TOKEN": "hf-secret"},
    api_token="sglang-endpoint-token",
    tensor_parallel_size=2,
    data_parallel_size=1,
)
```


## Running Examples

After deploying the connector, run the optimized examples:

```bash
# Basic HTTP server (uses CPU resources)
pyflyte run --remote examples/basic_inference.py basic_inference_workflow

# Advanced NIM deployment (uses GPU + advanced config)
pyflyte run --remote examples/nemotron_nim_example.py nemotron_super_workflow

# vLLM model deployment
pyflyte run --remote examples/vllm_example.py vllm_inference_workflow
```

## Monitoring & Debugging

```bash
# Monitor connector logs
kubectl logs -n flyte deployment/lepton-connector --follow

# Check Lepton console (URLs auto-generated)
# Links available in Flyte execution view

# List executions
pyflyte get executions -p flytesnacks -d development --limit 5
```

## Advanced Examples

### Complete NIM Workflow
```python
from flytekit import workflow
from flytekitplugins.dgxc_lepton import create_lepton_endpoint_task

nim_deployment = create_lepton_endpoint_task(
    deployment_type="nim",
    name="advanced_nim",
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    secrets={"NGC_API_KEY": "ngc-secret", "HF_TOKEN": "hf-secret"},
    api_token="unique-endpoint-token",
    scaling_mode="traffic",
    scale_to_zero=True,
    initial_delay_seconds=5000,
    mounts=[
        {
            "enabled": True,
            "cache_path": "/model-cache",
            "mount_path": "/opt/nim/.cache",
            "storage_source": "node-nfs:shared-fs"
        }
    ]
)

@workflow
def nim_workflow() -> str:
    nim_url = nim_deployment(
        endpoint_name="advanced-nim",
        resource_shape="gpu.2xh200",
        min_replicas=0,
        max_replicas=3,
        node_group="<your-node-group>",
    )
    return nim_url
```



## Configurations

### High-Level Abstractions

### Secret Management
Instead of complex nested dictionaries, use simple secret mapping:

```python
secrets={
    "HF_TOKEN": "huggingface-secret",
    "NGC_API_KEY": "ngc-secret",
    "CUSTOM_KEY": "custom-secret",
}
```

### Auto-scaling
Instead of complex auto_scaler dictionaries, use simple mode selection:

#### Traffic-based Scaling
```python
scaling_mode="traffic",
no_traffic_timeout=3600,  # 1 hour
scale_to_zero=False
```

#### GPU Utilization-based Scaling
```python
scaling_mode="gpu_utilization",
target_gpu_utilization=70,  # 70% GPU usage
scale_to_zero=False
```

#### QPM (Queries Per Minute) Scaling
```python
scaling_mode="qpm",
target_qpm=10.0,  # 10 queries per minute
scale_to_zero=True
```

### Secret Management
```python
secrets={
    "HF_TOKEN": "huggingface-secret",
    "NGC_API_KEY": "ngc-secret",
    "CUSTOM_KEY": "custom-secret",
}
```

### API Token Management
Instead of complex API token arrays, use simple token configuration:

```python
# Choose one:
api_token="my-unique-endpoint-token"         # Direct value
api_token_secret="endpoint-token-secret"     # From Lepton secret
```

### Health Configuration
```python
initial_delay_seconds=5000,  # Liveness probe delay
liveness_timeout=30,         # Liveness probe timeout
readiness_delay=300          # Readiness probe delay
```

### Important Scaling Constraints

**scale_to_zero**: Only works with `scaling_mode="traffic"`

```python
# Correct - scale_to_zero with traffic mode
scaling_mode="traffic", no_traffic_timeout=3600, scale_to_zero=True

# Invalid - scale_to_zero with other modes (will be ignored)
scaling_mode="gpu_utilization", scale_to_zero=True  # Won't work
scaling_mode="qpm", scale_to_zero=True              # Won't work
```

### Storage Mounts (Array Format with Individual Control)

**Single Mount**:
```python
mounts=[
    {
        "enabled": True,
        "cache_path": "/your/cache/path",
        "mount_path": "/opt/container/cache",
        "storage_source": "node-nfs:your-storage"
    }
]
```

**Multiple Mounts** (Each Individually Controllable):
```python
mounts=[
    {
        "enabled": True,   # This mount is active
        "cache_path": "/models-cache",
        "mount_path": "/opt/models",
        "storage_source": "node-nfs:model-storage"
    },
    {
        "enabled": False,  # This mount is disabled
        "cache_path": "/data-cache",
        "mount_path": "/opt/data",
        "storage_source": "node-nfs:data-storage"
    },
    {
        "enabled": True,   # This mount is active
        "cache_path": "/logs-cache",
        "mount_path": "/opt/logs",
        "storage_source": "node-nfs:logs-storage"
    }
]
```

## Configuration Requirements

**Important**: Replace these placeholders with your actual values:

- `<your-node-group>`: Your Kubernetes node group for GPU workloads (must be specified in task creation)
- `<your-ngc-secret>`: Your NGC registry pull secret name
- `/shared-storage/model-cache/*`: Your shared storage paths for model caching
- `NGC_API_KEY`: Your NGC API key secret name
- `HUGGING_FACE_HUB_TOKEN_read`: Your HuggingFace token secret name
- `<your-lepton-origin-url>`: Your Lepton workspace origin URL (e.g., `https://gateway.dgxc-lepton.nvidia.com`)

Example setup:
```bash
# Create your secrets
kubectl create secret generic ngc-api-key --from-literal=key=<your-ngc-key> -n flyte
kubectl create secret generic hf-token --from-literal=token=<your-hf-token> -n flyte

# Use in your deployment
env_vars={
    "NGC_API_KEY": {"value_from": {"secret_name_ref": "ngc-api-key"}},
    "HF_TOKEN": {"value_from": {"secret_name_ref": "hf-token"}},
}
```

## Plugin Structure

```
flytekit-dgxc-lepton/
├── README.md                          # This guide
├── setup.py                           # Package configuration
├── Dockerfile.connector               # Optimized connector image
├── deployment/                        # Kubernetes manifests
│   ├── connector-deployment.yaml     # Connector service
│   └── lepton-connector-config.yaml  # Flyte routing config
├── examples/                          # Clean usage examples
│   ├── basic_inference.py            # Simple deployment
│   ├── nemotron_nim_example.py       # Advanced NIM
│   └── vllm_example.py               # vLLM deployment
├── tests/                             # Test suite
│   └── test_lepton.py                 # Updated for optimized API
└── flytekitplugins/dgxc_lepton/      # Source code
    ├── __init__.py                   # Clean exports
    ├── connector.py                  # Optimized connector
    └── task.py                       # Unified task function
```

## Requirements

- **Runtime**: flytekit >= 1.9.1, leptonai (latest from GitHub)
- **Backend**: Kubernetes cluster with Flyte deployed
- **Credentials**: Lepton workspace ID and API token

## License

Apache License 2.0

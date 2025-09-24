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

# Create deployment tasks using the optimized unified API
http_server = create_lepton_endpoint_task(
    deployment_type="custom",  # Specify deployment type
    name="deploy_http_server",
    image="python:3.11-slim",
    command=["/bin/bash", "-c", "python3 -m http.server 8080 --bind 0.0.0.0"],
)

# Deploy vLLM with optimized defaults
vllm_server = create_lepton_endpoint_task(
    deployment_type="vllm",    # Automatically sets vLLM + GPU defaults
    name="deploy_vllm_server",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="llama-3.1-8b-instruct",
)

@workflow
def inference_workflow(endpoint_name: str = "my-endpoint") -> str:
    # Only specify what's different - defaults are auto-applied
    endpoint_url = http_server(
        endpoint_name=endpoint_name,
        deployment_type="custom",  # Required parameter
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
    port=8080,  # Overrides default 8000 for custom containers
    command=["/bin/bash", "-c", "python3 -m http.server 8080 --bind 0.0.0.0"]
)
```

### 2. NVIDIA NIM
```python
nim_task = create_lepton_endpoint_task(
    deployment_type="nim",
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    resource_shape="gpu.2xh200",  # Override default gpu.1xh200
    min_replicas=0,  # Scale to zero when not in use
    health_config={
        "liveness": {"initial_delay_seconds": 5000}  # Large model needs time
    },
    env_vars={
        "NGC_API_KEY": {"value_from": {"secret_name_ref": "NGC_API_KEY"}},
        "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_TOKEN"}},
    }
)
```

### 3. vLLM
```python
vllm_task = create_lepton_endpoint_task(
    deployment_type="vllm",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="llama-3.1-8b",
    tensor_parallel_size=2,
    extra_args="--gpu-memory-utilization 0.95 --trust-remote-code"
)
```

### 4. SGLang
```python
sglang_task = create_lepton_endpoint_task(
    deployment_type="sglang",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    tensor_parallel_size=2,
    data_parallel_size=1
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

# Advanced NIM with comprehensive configuration
nim_deployment = create_lepton_endpoint_task(
    deployment_type="nim",
    name="advanced_nim",
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    resource_shape="gpu.2xh200",
    min_replicas=0,  # Scale to zero
    health_config={
        "liveness": {"initial_delay_seconds": 5000}  # Large model initialization
    },
    env_vars={
        "NGC_API_KEY": {"value_from": {"secret_name_ref": "NGC_API_KEY"}},
        "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_TOKEN"}},
    },
    mounts={
        "enabled": True,
        "cache_path": "/model-cache",
        "mount_path": "/opt/nim/.cache",
        "storage_source": "node-nfs:shared-fs"
    }
)

@workflow
def nim_workflow() -> str:
    # Clean workflow - all config in task definition
    nim_url =  nim_deployment(
        endpoint_name="advanced-nim",
    )
    return nim_url
```

### Multi-Model Deployment
```python
@workflow
def multi_model_workflow() -> str:
    # Deploy multiple models in parallel
    custom_url = create_lepton_endpoint_task("custom", image="python:3.11-slim")(
        endpoint_name="multi-custom",
        deployment_type="custom"
    )

    nim_url = create_lepton_endpoint_task("nim", resource_shape="gpu.2xh200")(
        endpoint_name="multi-nim",
        deployment_type="nim"
    )

    return f"Custom: {custom_url} | NIM: {nim_url}"
```


## Configurations

### Auto-scaling Configuration

**Important**: Only **one** auto-scaling method can be active at a time. Set unused methods to 0.

#### Traffic-based Scaling (Default)
```python
auto_scaler={
    "scale_down": {"no_traffic_timeout": 3600, "scale_from_zero": False},
    "target_gpu_utilization_percentage": 0,  # Disabled
    "target_throughput": {"qpm": 0, "paths": [], "methods": []}  # Disabled
}
```

#### GPU Utilization-based Scaling
```python
auto_scaler={
    "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},  # Disabled
    "target_gpu_utilization_percentage": 70,  # Scale based on GPU usage
    "target_throughput": {"qpm": 0, "paths": [], "methods": []}  # Disabled
}
```

#### QPM (Queries Per Minute) Scaling
```python
auto_scaler={
    "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},  # Disabled
    "target_gpu_utilization_percentage": 0,  # Disabled
    "target_throughput": {"qpm": 10.0, "paths": [], "methods": []}  # Scale based on request rate
}
```

### Environment Variables & Secrets
```python
env_vars={
    "DIRECT_VALUE": "some_value",
    "SECRET_VALUE": {"value_from": {"secret_name_ref": "MY_SECRET"}}
}
```

### Storage Mounts
```python
mounts={
    "enabled": True,
    "cache_path": "/shared-storage/model-cache",  # Your shared storage path
    "mount_path": "/opt/nim/.cache",              # Container mount point
    "storage_source": "node-nfs:your-storage"    # Your storage source
}
```

## Configuration Requirements

**Important**: Replace these placeholders with your actual values:

- `<your-node-group>`: Your Kubernetes node group for GPU workloads
- `<your-ngc-secret>`: Your NGC registry pull secret name
- `/shared-storage/model-cache/*`: Your shared storage paths for model caching
- `NGC_API_KEY`: Your NGC API key secret name
- `HUGGING_FACE_HUB_TOKEN_read`: Your HuggingFace token secret name

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

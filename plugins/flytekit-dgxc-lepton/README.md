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
- `leptonai` package installed in the agent environment
- Properly configured Lepton credentials

### Credentials Setup
Configure these secrets in your Flyte deployment:
- `lepton_workspace_id`: Your Lepton workspace identifier
- `lepton_token`: Your Lepton API authentication token

Users do not need local Lepton CLI installation - all operations are handled by the backend agent.

## Deployment

### Deploy the Agent Service

Deploy the dgxc-lepton connector as a dedicated agent service:

```bash
kubectl apply -f deployment/agent-deployment.yaml
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
kubectl logs -n flyte deployment/lepton-agent | grep "Lepton Endpoint Connector"
```

You should see the connector listed in the supported task types.

## Quick Start

```python
from flytekit import task, workflow
from flytekitplugins.dgxc_lepton import LeptonConfig

@task(task_config=LeptonConfig(
    endpoint_name="my-inference-model",
    image="python:3.11-slim",
    deployment_type="custom",
    resource_shape="gpu.a10",
    min_replicas=1,
    max_replicas=3
))
def inference_task(input_data: str) -> str:
    return f"Processed: {input_data}"

@workflow
def inference_workflow(data: str) -> str:
    return inference_task(input_data=data)
```

## Running Examples

After deploying the agent, you can run the provided examples:

### Basic Inference Example

```bash
# Run the basic HTTP server example
pyflyte run -p flytesnacks -d development --remote examples/basic_inference.py basic_inference_workflow --input_text "Hello World"
```

### Advanced Examples

```bash
# Run NIM inference example
pyflyte run -p flytesnacks -d development --remote examples/advanced_usage.py nim_inference_workflow --prompt "What is artificial intelligence?"

# Run vLLM inference example
pyflyte run -p flytesnacks -d development --remote examples/advanced_usage.py vllm_inference_workflow --prompt "Explain neural networks"

# Run multi-model comparison workflow
pyflyte run -p flytesnacks -d development --remote examples/advanced_usage.py multi_model_comparison_workflow --prompt "Compare machine learning frameworks"


```

### Individual vLLM Example

```bash
# Run the standalone vLLM example
pyflyte run -p flytesnacks -d development --remote examples/vllm_example.py vllm_inference_workflow --prompt "What is machine learning?"
```


### Monitor Execution

You can monitor the execution in the Flyte UI or via CLI:

```bash
# List recent executions
pyflyte get executions -p flytesnacks -d development --limit 5

# Watch specific execution logs
kubectl logs -n flyte deployment/lepton-agent --follow
```

### Expected Behavior

1. **Task Creation**: Agent creates Lepton endpoint with specified configuration
2. **Status Polling**: Agent monitors endpoint until it becomes ready
3. **Success**: Task completes with the endpoint URL as output

The task output will contain the actual Lepton endpoint URL for making inference requests.

## Container-based Deployments

### NIM Deployment
```python
@task(task_config=LeptonConfig(
    endpoint_name="nim-llama-3.1-8b-instruct",
    image="nvcr.io/nim/meta/llama-3.1-8b-instruct:1.8.6",
    deployment_type="nim",
    port=8000,
    served_model_name="meta/llama-3.1-8b-instruct",
    resource_shape="gpu.1xh200",
    min_replicas=1,
    max_replicas=3,
    node_group="nv-int-multiteam-nebius-h200-01",

    # Environment variables with secret references
    env_vars={
        "OMPI_ALLOW_RUN_AS_ROOT": "1",
        "NGC_API_KEY": {"value_from": {"secret_name_ref": "NGC_API_KEY"}},
        "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_TOKEN"}},
    },

    # API tokens for endpoint access
    api_tokens=[{"value": "UNIQUE_ENDPOINT_TOKEN"}],

    # Auto-scaling configuration
    auto_scaler={
        "scale_down": {"no_traffic_timeout": 300, "scale_from_zero": False},
        "target_gpu_utilization_percentage": 70,
        "target_throughput": {"qpm": 2.5, "paths": [], "methods": []}
    },

    # Storage mounts for model caching
    mounts={
        "enabled": True,
        "cache_path": "/path/to/model/cache",
        "mount_path": "/opt/nim/.cache",
        "storage_source": "node-nfs:lepton-shared-fs"
    },

    # Image pull secrets for private registries
    image_pull_secrets=["my-registry-secret"],

    endpoint_readiness_timeout=1800  # 30 minutes
))
def nim_inference_task(prompt: str) -> str:
    # Your NIM inference logic here
    return f"Generated response for: {prompt}"

### vLLM Deployment
```python
@task(task_config=LeptonConfig(
    endpoint_name="vllm-llama-3.1-8b-eval",
    image="vllm/vllm-openai:latest",
    deployment_type="vllm",
    port=8000,

    # Model configuration (required for vLLM)
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="llama-3.1-8b-instruct",
    tensor_parallel_size=1,
    pipeline_parallel_size=1,
    data_parallel_size=1,
    extra_args="--gpu-memory-utilization 0.95 --trust-remote-code",

    resource_shape="gpu.1xh200",
    min_replicas=1,
    max_replicas=3,

    env_vars={
        "HF_TOKEN": {"value_from": {"secret_name_ref": "HUGGING_FACE_TOKEN"}}
    }
))
def vllm_inference_task(prompt: str) -> str:
    # Your vLLM inference logic here
    return f"vLLM response for: {prompt}"

### SGLang Deployment
```python
@task(task_config=LeptonConfig(
    endpoint_name="sglang-model",
    image="lmsysorg/sglang:latest",
    deployment_type="sglang",

    # Model configuration (required for SGLang)
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="llama-3.1-8b-instruct",
    tensor_parallel_size=1,
    data_parallel_size=1,

    resource_shape="gpu.a100",
    min_replicas=1
))
def sglang_inference_task(prompt: str) -> str:
    return f"SGLang response for: {prompt}"
```

## Configuration Options

### Core Configuration
- `endpoint_name`: Name of the Lepton endpoint to deploy
- `image`: Container image to deploy (required for container-based deployments)
- `deployment_type`: Deployment type ("nim", "vllm", "sglang", or "custom")
- `port`: Container port (default: 8080)
- `resource_shape`: Resource shape (e.g., "cpu.small", "gpu.a10", "gpu.1xh200")
- `min_replicas`/`max_replicas`: Replica configuration

### Model Configuration (for vLLM/SGLang)
- `checkpoint_path`: Model path or HuggingFace model ID (optional)
- `served_model_name`: Name for the served model (optional)
- `tensor_parallel_size`: Number of tensor parallel processes (optional, not used for NIM)
- `pipeline_parallel_size`: Number of pipeline parallel stages (optional, vLLM only)
- `data_parallel_size`: Number of data parallel processes (optional, not used for NIM)
- `extra_args`: Additional command line arguments (optional)

### Environment Variables
Supports both direct values and secret references:
```python
env_vars={
    "DIRECT_VALUE": "some_value",
    "SECRET_VALUE": {"value_from": {"secret_name_ref": "MY_SECRET"}}
}
```

### Auto-scaling
```python
auto_scaler={
    "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": True},
    "target_gpu_utilization_percentage": 70,
    "target_throughput": {"qpm": 0, "paths": [], "methods": []}
}
```

### Storage Mounts
```python
mounts={
    "enabled": True,
    "cache_path": "/host/path/to/cache",
    "mount_path": "/container/mount/path",
    "storage_source": "node-nfs:storage-name"
}
```

### Other Options
- `node_group`: Node group for workload placement
- `api_tokens`: API tokens for endpoint access
- `image_pull_secrets`: Secrets for private registries
- `deployment_timeout`: Deployment operation timeout
- `endpoint_readiness_timeout`: Endpoint readiness timeout

## Files in This Plugin

```
flytekit-dgxc-lepton/
├── README.md                          # This comprehensive guide
├── setup.py                           # Package configuration with leptonai dependency
├── Dockerfile.agent                   # Docker build for dedicated agent service
├── deployment/                        # Kubernetes deployment files
│   ├── agent-deployment.yaml         # Agent deployment with environment variables
│   └── lepton-agent-config.yaml      # Flyte configuration for routing tasks
├── examples/                          # Usage examples
│   ├── basic_inference.py            # Simple HTTP server endpoint
│   ├── advanced_usage.py             # Comprehensive examples (Basic, NIM, vLLM)
│   └── vllm_example.py               # Individual vLLM deployment
└── tests/                             # Test suite
    └── test_lepton.py                 # Comprehensive tests (7 passing)
```

## Requirements

- **User Environment**: flytekit >= 1.9.1
- **Backend Environment**: flytekit >= 1.9.1, leptonai (auto-installed via setup.py)

## License

Apache License 2.0

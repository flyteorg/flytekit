# Flytekit DGXC Lepton Plugin

A professional Flytekit plugin that enables seamless deployment and management of AI inference endpoints using Lepton AI infrastructure within Flyte workflows.

## Overview

This plugin provides:
- **Unified Task API** for deployment and management of Lepton AI endpoints
- **Type-safe configuration** with consolidated dataclasses and IDE support
- **Multiple endpoint engines**: VLLM, SGLang, NIM, and custom containers
- **Unified configuration classes** for scaling, environment, and mounts

## Installation

```bash
pip install flytekitplugins-dgxc-lepton
```

## Quick Start

```python
from flytekit import workflow
from flytekitplugins.dgxc_lepton import (
    lepton_endpoint_deployment_task, lepton_endpoint_deletion_task, LeptonEndpointConfig,
    EndpointEngineConfig, EnvironmentConfig, ScalingConfig
)

@workflow
def inference_workflow() -> str:
    """Deploy Llama model using VLLM and return endpoint URL."""

    # Complete configuration in one place
    config = LeptonEndpointConfig(
        endpoint_name="my-llama-endpoint",
        resource_shape="gpu.1xh200",
        node_group="your-node-group",
        endpoint_config=EndpointEngineConfig.vllm(
            checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
            served_model_name="llama-3.1-8b-instruct",
        ),
        environment=EnvironmentConfig.create(
            LOG_LEVEL="INFO",
            secrets={"HF_TOKEN": "hf-secret"}
        ),
        scaling=ScalingConfig.traffic(min_replicas=1, max_replicas=2),
    )

    # Deploy endpoint and return URL
    return lepton_endpoint_deployment_task(config=config)
```

## API Reference

### Core Components

#### `lepton_endpoint_deployment_task(config: LeptonEndpointConfig) -> str`
Main function for Lepton AI endpoint deployment.

**Parameters:**
- `config`: Complete endpoint configuration
- `task_name`: Optional custom task name

**Returns:**
- Endpoint URL for successful deployment

#### `lepton_endpoint_deletion_task(endpoint_name: str, ...) -> str`
Function for Lepton AI endpoint deletion.

**Parameters:**
- `endpoint_name`: Name of the endpoint to delete
- `task_name`: Optional custom task name

**Returns:**
- Success message confirming deletion

#### `LeptonEndpointConfig`
Unified configuration for all Lepton endpoint operations.

**Required Fields:**
- `endpoint_name`: Name of the endpoint
- `resource_shape`: Hardware resource specification (e.g., "gpu.1xh200")
- `node_group`: Target node group for deployment
- `endpoint_config`: Engine-specific configuration

**Optional Fields:**
- `scaling`: Auto-scaling configuration
- `environment`: Environment variables and secrets
- `mounts`: Storage mount configurations
- `api_token`/`api_token_secret`: Authentication
- `image_pull_secrets`: Container registry secrets
- `endpoint_readiness_timeout`: Deployment timeout

### Endpoint Engine Configuration

#### `EndpointEngineConfig`
Unified configuration for different inference engines.


##### VLLM Engine
```python
EndpointEngineConfig.vllm(
    image="vllm/vllm-openai:latest",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    served_model_name="default-model",
    tensor_parallel_size=1,
    pipeline_parallel_size=1,
    data_parallel_size=1,
    extra_args="--max-model-len 4096",
    port=8000
)
```

##### SGLang Engine
```python
EndpointEngineConfig.sglang(
    image="lmsysorg/sglang:latest",
    checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
    tensor_parallel_size=1,
    data_parallel_size=1,
    extra_args="--context-length 4096",
    port=30000
)
```

##### NVIDIA NIM
```python
EndpointEngineConfig.nim(
    image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest",
    port=8000
)
```

##### Custom Container
```python
EndpointEngineConfig.custom(
    image="python:3.11-slim",
    command=["/bin/bash", "-c", "python3 -m http.server 8080"],
    port=8080
)
```

### Scaling Configuration

#### `ScalingConfig`
Unified auto-scaling configuration with enforced single strategy.


##### Traffic-based Scaling
```python
ScalingConfig.traffic(
    min_replicas=1,
    max_replicas=5,
    timeout=1800  # Scale down after 30 min of no traffic
)
```

##### GPU Utilization Scaling
```python
ScalingConfig.gpu(
    target_utilization=80,  # Target 80% GPU utilization
    min_replicas=1,
    max_replicas=10
)
```

##### QPM (Queries Per Minute) Scaling
```python
ScalingConfig.qpm(
    target_qpm=100.5,  # Target queries per minute
    min_replicas=2,
    max_replicas=8
)
```

### Environment Configuration

#### `EnvironmentConfig`
Unified configuration for environment variables and secrets.

**Factory Methods:**

##### Environment Variables Only
```python
EnvironmentConfig.from_env(
    LOG_LEVEL="DEBUG",
    MODEL_PATH="/models",
    CUDA_VISIBLE_DEVICES="0,1"
)
```

##### Secrets Only
```python
EnvironmentConfig.from_secrets(
    HF_TOKEN="hf-secret",
    NGC_API_KEY="ngc-secret"
)
```

##### Mixed Configuration
```python
EnvironmentConfig.create(
    LOG_LEVEL="INFO",
    MODEL_PATH="/models",
    secrets={
        "HF_TOKEN": "hf-secret",
        "NGC_API_KEY": "ngc-secret"
    }
)
```

### Mount Configuration

#### `MountReader`
Simplified NFS mount configuration.

```python
MountReader.node_nfs(
    ("/shared-storage/models", "/opt/models"),
    ("/shared-storage/data", "/opt/data"),
    ("/shared-storage/logs", "/opt/logs", False),  # Disabled mount
    storage_name="production-nfs"  # Custom storage name
)
```

## Complete Examples

### VLLM Deployment with Auto-scaling

```python
from flytekit import workflow
from flytekitplugins.dgxc_lepton import (
    lepton_endpoint_deployment_task, LeptonEndpointConfig,
    EndpointEngineConfig, EnvironmentConfig, ScalingConfig, MountReader
)

@workflow
def deploy_vllm_with_scaling() -> str:
    """Deploy VLLM with traffic-based auto-scaling."""

    config = LeptonEndpointConfig(
        endpoint_name="vllm-llama-3.1-8b",
        resource_shape="gpu.1xh200",
        node_group="inference-nodes",
        endpoint_config=EndpointEngineConfig.vllm(
            checkpoint_path="meta-llama/Llama-3.1-8B-Instruct",
            served_model_name="llama-3.1-8b-instruct",
            tensor_parallel_size=1,
            extra_args="--max-model-len 8192 --enable-chunked-prefill"
        ),
        environment=EnvironmentConfig.create(
            LOG_LEVEL="INFO",
            CUDA_VISIBLE_DEVICES="0",
            secrets={"HF_TOKEN": "hf-secret"}
        ),
        scaling=ScalingConfig.traffic(
            min_replicas=1,
            max_replicas=3,
            timeout=1800
        ),
        mounts=MountReader.node_nfs(
            ("/shared-storage/models", "/opt/models"),
            ("/shared-storage/cache", "/root/.cache")
        ),
        api_token_secret="lepton-api-token",
        image_pull_secrets=["hf-secret"],
        endpoint_readiness_timeout=600
    )

    return lepton_endpoint_deployment_task(config=config)
```

### NIM Deployment with QPM Scaling

```python
@workflow
def deploy_nim_with_qpm_scaling() -> str:
    """Deploy NVIDIA NIM with QPM-based scaling."""

    config = LeptonEndpointConfig(
        endpoint_name="nemotron-super-reasoning",
        resource_shape="gpu.1xh200",
        node_group="nim-nodes",
        endpoint_config=EndpointEngineConfig.nim(
            image="nvcr.io/nim/nvidia/llama-3_3-nemotron-super-49b-v1_5:latest"
        ),
        environment=EnvironmentConfig.create(
            OMPI_ALLOW_RUN_AS_ROOT="1",
            secrets={"NGC_API_KEY": "ngc-secret"}
        ),
        scaling=ScalingConfig.qpm(
            target_qpm=2.5,
            min_replicas=1,
            max_replicas=3
        ),
        image_pull_secrets=["ngc-secret"],
        api_token="UNIQUE_ENDPOINT_TOKEN"
    )

    return lepton_endpoint_deployment_task(config=config)
```

### Custom Container Deployment

```python
@workflow
def deploy_custom_service() -> str:
    """Deploy custom inference service."""

    config = LeptonEndpointConfig(
        endpoint_name="custom-inference-api",
        resource_shape="cpu.large",
        node_group="cpu-nodes",
        endpoint_config=EndpointEngineConfig.custom(
            image="my-registry/inference-api:v1.0",
            command=["python", "app.py"],
            port=8080
        ),
        environment=EnvironmentConfig.from_env(
            LOG_LEVEL="DEBUG",
            API_VERSION="v1",
            WORKERS="4"
        ),
        scaling=ScalingConfig.gpu(
            target_utilization=70,
            min_replicas=2,
            max_replicas=6
        )
    )

    return lepton_endpoint_deployment_task(config=config)
```
## Configuration Requirements

Replace these placeholders with your actual values:
- `<your-node-group>`: Your Kubernetes node group for GPU workloads
- `<your-ngc-secret>`: Your NGC registry pull secret name
- `/shared-storage/model-cache/*`: Your shared storage paths for model caching
- `NGC_API_KEY`: Your NGC API key secret name
- `HUGGING_FACE_HUB_TOKEN_read`: Your HuggingFace token secret name

## Monitoring & Debugging

```bash
# Monitor connector logs
kubectl logs -n flyte deployment/lepton-connector --follow

# Check Lepton console (URLs auto-generated in Flyte execution view)

# List recent executions
pyflyte get executions -p flytesnacks -d development --limit 5

## Development

### Running Tests

```bash
pytest tests/test_lepton.py -v
```

### Plugin Registration

The plugin automatically registers with Flytekit's dynamic plugin loading system:

```python
# Automatic registration enables this usage pattern
task = LeptonEndpointDeploymentTask(config=config)
```

## Support

For issues, questions, or contributions, please refer to the Flytekit documentation and Lepton AI platform documentation.

## License

This plugin follows the same license as Flytekit.

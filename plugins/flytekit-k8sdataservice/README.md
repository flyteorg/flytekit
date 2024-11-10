# K8s Stateful Service Plugin

This plugin provides support for Kubernetes StatefulSet and Service integration, enabling seamless provisioning and coordination with any Kubernetes services or Flyte tasks. It is especially suited for deep learning use cases at scale, where distributed and parallelized data loading and caching across nodes are required.

## Features
- **Predictable and Reliable Endpoints**: The service creates consistent endpoints, facilitating communication between services or tasks within the same Kubernetes cluster.
- **Reusable Across Runs**: Service tasks can persist across task runs, ensuring consistency. Alternatively, a cleanup sensor can release cluster resources when they are no longer needed.
- **Conventional Pod Naming**: Pods in the StatefulSet follow a conventional naming pattern. For instance, if the StatefulSet name is `foo` and replicas are set to 2, the pod endpoints will be `foo-0.foo:1234` and `foo-1.foo:1234`. This simplifies endpoint construction for training or inference scripts. For example, gRPC endpoints can directly use `foo-0.foo:1234` and `foo-1.foo:1234`.

## Installation

Install the plugin via pip:

```bash
pip install flytekitplugins-k8sdataservice
```

## Usage

Below is an example demonstrating how to provision and run a service in Kubernetes, making it reachable within the cluster.

**Note**: Utility functions are available to generate unique service names that can be reused across training or inference scripts.

### Example Usage

#### Provisioning a Data Service
```python
from flytekitplugins.k8sdataservice import DataServiceConfig, DataServiceTask, CleanupSensor
from utils.infra import gen_infra_name
from flytekit import kwtypes, Resources, task, workflow

# Generate a unique infrastructure name
name = gen_infra_name()

def k8s_data_service():
    gnn_config = DataServiceConfig(
        Name=name,
        Requests=Resources(cpu='1', mem='1Gi'),
        Limits=Resources(cpu='2', mem='2Gi'),
        Replicas=1,
        Image="busybox:latest",
        Command=[
            "bash",
            "-c",
            "echo Hello Flyte K8s Stateful Service! && sleep 3600"
        ],
    )

    gnn_task = DataServiceTask(
        name="K8s Stateful Data Service",
        inputs=kwtypes(ds=str),
        task_config=gnn_config,
    )
    return gnn_task

# Define a cleanup sensor
gnn_sensor = CleanupSensor(name="Cleanup")

# Define a workflow to test the data service
@workflow
def test_dataservice_wf(name: str):
    k8s_data_service()(ds="OSS Flyte K8s Data Service Demo") \
          >> gnn_sensor(
              release_name=name,
              cleanup_data_service=True,
          )

if __name__ == "__main__":
    out = test_dataservice_wf(name="example")
    print(f"Running test_dataservice_wf() {out}")
```

#### Accessing the Data Service
Other tasks or services that need to access the data service can do so in multiple ways. For example, using environment variables:

```python
from kubernetes.client import V1PodSpec, V1Container, V1EnvVar

PRIMARY_CONTAINER_NAME = "primary"
FLYTE_POD_SPEC = V1PodSpec(
    containers=[
        V1Container(
            name=PRIMARY_CONTAINER_NAME,
            env=[
                V1EnvVar(name="MY_DATASERVICES", value=f"{name}-0.{name}:40000 {name}-1.{name}:40000"),
            ],
        )
    ],
)

task_config = MPIJob(
    launcher=Launcher(replicas=1, pod_template=FLYTE_POD_SPEC),
    worker=Worker(replicas=1, pod_template=FLYTE_POD_SPEC),
)

@task(task_config=task_config)
def mpi_task() -> str:
    return "your script uses the envs to communicate with the data service "
```

### Key Points
- The `DataServiceConfig` defines resource requests, limits, replicas, and the container image/command.
- The `CleanupSensor` ensures resources are cleaned up when required.
- The workflow connects the service provisioning and cleanup process for streamlined operations.

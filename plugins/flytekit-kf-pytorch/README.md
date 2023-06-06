# Flytekit Kubeflow PyTorch Plugin

This plugin uses the Kubeflow PyTorch Operator and provides an extremely simplified interface for executing distributed training using various PyTorch backends.

This plugin can execute torch elastic training, which is equivalent to run `torchrun`. Elastic training can be executed
in a single Pod (without requiring the PyTorch operator, see below) as well as in a distributed multi-node manner.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-kfpytorch
```

To set up PyTorch operator in the Flyte deployment's backend, follow the [PyTorch Operator Setup](https://docs.flyte.org/en/latest/deployment/plugin_setup/pytorch_operator.html) guide.

An [example](https://docs.flyte.org/projects/cookbook/en/latest/auto/integrations/kubernetes/kfpytorch/pytorch_mnist.html#sphx-glr-auto-integrations-kubernetes-kfpytorch-pytorch-mnist-py) showcasing PyTorch operator can be found in the documentation.

## Code Example
```python
from flytekitplugins.kfpytorch import PyTorch, Worker, Master, RestartPolicy, RunPolicy, CleanPodPolicy

@task(
    task_config = PyTorch(
        worker=Worker(
            replicas=5,
            requests=Resources(cpu="2", mem="2Gi"),
            limits=Resources(cpu="4", mem="2Gi"),
            image="worker:latest",
            restart_policy=RestartPolicy.FAILURE,
        ),
        master=Master(
            restart_policy=RestartPolicy.ALWAYS,
        ),
    )
    image="test_image",
    resources=Resources(cpu="1", mem="1Gi"),
)
def pytorch_job():
    ...
```


## Upgrade Pytorch Plugin from V0 to V1
Pytorch plugin is now updated from v0 to v1 to enable more configuration options.
To migrate from v0 to v1, change the following:
1. Update flytepropeller to v1.6.0
2. Update flytekit version to v1.6.2
3. Update your code from:
    ```
    task_config=Pytorch(num_workers=10),
    ```
    to:
    ```
    task_config=PyTorch(worker=Worker(replicas=10)),
    ```

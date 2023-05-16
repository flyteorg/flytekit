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

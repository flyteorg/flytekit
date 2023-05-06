# Flytekit Kubeflow TensorFlow Plugin

This plugin uses the Kubeflow TensorFlow Operator and provides an extremely simplified interface for executing distributed training using various TensorFlow backends.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-kftensorflow
```

## Upgrade TensorFlow Plugin
Tensorflow plugin is now updated from v0 to v1 to enable more configuration options.
To migrate from v0 to v1, change the following:
1. Update flytepropeller to v
2. Update flytekit version to v
3. Update your code from:
    ```
    task_config=TfJob(num_workers=10, num_ps_replicas=1, num_chief_replicas=1),
    ```
    to:
    ```
    task_config=TfJob(worker=Worker(replicas=10), ps=PS(replicas=1), chief=Chief(replicas=1)),
    ```

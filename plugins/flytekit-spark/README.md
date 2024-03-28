# Flytekit Spark Plugin

Flyte can execute Spark jobs natively on a Kubernetes Cluster, which manages a virtual cluster’s lifecycle, spin-up, and tear down. It leverages the open-sourced Spark On K8s Operator and can be enabled without signing up for any service. This is like running a transient spark cluster — a type of cluster spun up for a specific Spark job and torn down after completion.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-spark
```

To configure Spark in the Flyte deployment's backend, follow [Step 1](https://docs.flyte.org/en/latest/deployment/plugins/k8s/index.html#deployment-plugin-setup-k8s), [2](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/index.html).

All [examples](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/index.html) showcasing execution of Spark jobs using the plugin can be found in the documentation.

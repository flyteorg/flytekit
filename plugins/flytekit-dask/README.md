# Flytekit Dask Plugin

Flyte can execute `dask` jobs natively on a Kubernetes Cluster, which manages the virtual `dask` cluster's lifecycle
(spin-up and tear down). It leverages the open-source Kubernetes Dask Operator and can be enabled without signing up
for any service. This is like running a transient (ephemeral) `dask` cluster - a type of cluster spun up for a specific
task and torn down after completion. This helps in making sure that the Python environment is the same on the job-runner
(driver), scheduler and the workers.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-dask
```

To configure Dask in the Flyte deployment's backed, follow
[these directions](https://docs.flyte.org/en/latest/deployment/plugins/k8s/index.html)ernetes/k8s_dask/index.html#step-2-environment-setup)

An [usage example](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_dask_plugin/index.html)
can be found in the documentation.

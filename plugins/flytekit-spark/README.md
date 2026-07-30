# Flytekit Spark Plugin

Flyte can execute Spark jobs natively on a Kubernetes Cluster, which manages a virtual cluster’s lifecycle, spin-up, and tear down. It leverages the open-sourced Spark On K8s Operator and can be enabled without signing up for any service. This is like running a transient spark cluster — a type of cluster spun up for a specific Spark job and torn down after completion.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-spark
```

To configure Spark in the Flyte deployment's backend, follow [Step 1](https://docs.flyte.org/en/latest/deployment/plugins/k8s/index.html#deployment-plugin-setup-k8s), [2](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/index.html).

All [examples](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/index.html) showcasing execution of Spark jobs using the plugin can be found in the documentation.

## Databricks authentication

The Databricks connector uses PAT authentication by default, preserving the
existing `databricks-token` namespace Secret and
`FLYTE_DATABRICKS_ACCESS_TOKEN` fallback.

OAuth machine-to-machine (M2M) authentication can be enabled on the connector:

```yaml
env:
  - name: FLYTE_DATABRICKS_AUTH_TYPE
    value: oauth_m2m
  - name: DATABRICKS_CLIENT_ID
    value: "<service-principal-client-id>"
  - name: DATABRICKS_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: databricks-connector-oauth
        key: client_secret
```

For per-namespace identities, create a `databricks-oauth` Secret in each
workflow namespace:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: databricks-oauth
  namespace: "<workflow-namespace>"
type: Opaque
stringData:
  client_id: "<service-principal-client-id>"
  client_secret: "<service-principal-client-secret>"
```

The namespace Secret takes precedence over connector-level credentials.
`get` and `delete` operations cache short-lived OAuth tokens and retry once
with a refreshed token when the Databricks API returns HTTP 401.

# Flytekit Spark Plugin

Flyte can execute Spark jobs natively on a Kubernetes Cluster, which manages a virtual cluster’s lifecycle, spin-up, and tear down. It leverages the open-sourced Spark On K8s Operator and can be enabled without signing up for any service. This is like running a transient spark cluster — a type of cluster spun up for a specific Spark job and torn down after completion.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-spark
```

To configure Spark in the Flyte deployment's backend, follow [Step 1](https://docs.flyte.org/en/latest/deployment/plugins/k8s/index.html#deployment-plugin-setup-k8s), [2](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/index.html).

All [examples](https://docs.flyte.org/en/latest/flytesnacks/examples/k8s_spark_plugin/index.html) showcasing execution of Spark jobs using the plugin can be found in the documentation.

## Databricks Connector Authentication

The `DatabricksV2` task config drives Flyte's Databricks connector, which calls the Databricks Jobs REST API to submit runs. The connector supports three authentication types:

| Auth type         | Credentials                                  | Typical use case                                        |
|-------------------|----------------------------------------------|---------------------------------------------------------|
| `pat`             | Personal Access Token                        | Existing deployments; simplest to configure             |
| `oauth_m2m`       | Service Principal `client_id` + `client_secret` | Shared credentials across many workflows                |
| `oidc_federation` | Workload Identity JWT (IRSA / Kubernetes SA) | No long-lived secrets; identity tied to the workload    |

All three produce an `Authorization: Bearer <token>` header for the Jobs API. The choice is transparent to the workflow code — you can migrate between modes by changing connector deployment config without touching any Python tasks.

### Resolution order

For every auth-related field (`databricks_auth_type`, `databricks_client_id`, ...), the connector checks sources in this order and uses the first hit:

1. The task config field on `DatabricksV2(...)`.
2. The corresponding `FLYTE_DATABRICKS_*` or `DATABRICKS_*` environment variable on the connector pod.
3. A well-known default (e.g. `databricks-oauth` for the OAuth k8s secret name, `databricks` for the OIDC audience).

If `FLYTE_DATABRICKS_AUTH_TYPE` is not set at all, the connector auto-detects an auth type per task: OIDC federation if a subject JWT file and a `client_id` are reachable, else OAuth M2M if `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` are set, else PAT. This means that in the common case the operator only needs to set connector env vars and mount the right IRSA/secret — workflows don't change.

For OIDC federation specifically, the choice between Model 2 (per-workflow-namespace SA) and Model 1 (connector pod identity) is made automatically at submit time based on whether an annotated ServiceAccount is found in the workflow's namespace. See the OIDC Federation section below.

### Environment variables (connector defaults)

| Variable                                        | Purpose                                                 |
|-------------------------------------------------|---------------------------------------------------------|
| `FLYTE_DATABRICKS_INSTANCE`                     | Default Databricks workspace hostname                   |
| `FLYTE_DATABRICKS_AUTH_TYPE`                    | `pat` / `oauth_m2m` / `oidc_federation` (optional)      |
| `FLYTE_DATABRICKS_ACCESS_TOKEN`                 | Fallback PAT if no namespace secret is found            |
| `FLYTE_DATABRICKS_TOKEN_SECRET_NAME`            | k8s secret name holding the PAT (default `databricks-token`) |
| `FLYTE_DATABRICKS_OAUTH_SECRET_NAME`            | k8s secret with `client_id` / `client_secret` keys (default `databricks-oauth`) |
| `DATABRICKS_CLIENT_ID`                          | Service Principal client ID for M2M / OIDC              |
| `DATABRICKS_CLIENT_SECRET`                      | Service Principal client secret for M2M                 |
| `AWS_WEB_IDENTITY_TOKEN_FILE`                   | IRSA-injected subject JWT (for OIDC Model 1)            |
| `FLYTE_DATABRICKS_OIDC_TOKEN_FILE`              | Override path to subject JWT (for OIDC Model 1)         |
| `FLYTE_DATABRICKS_OIDC_AUDIENCE`                | OIDC audience (default `databricks`); per-namespace override via SA annotation |
| `FLYTE_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER`  | Provider name for serverless S3 credentials             |

### PAT (Personal Access Token)

Default and simplest mode. The connector looks up a k8s secret named `databricks-token` in the workflow namespace, and falls back to `FLYTE_DATABRICKS_ACCESS_TOKEN` on the connector pod.

Per-task override:

```python
DatabricksV2(
    databricks_conf={...},
    databricks_instance="my-workspace.cloud.databricks.com",
    databricks_token_secret="team-a-databricks-token",
)
```

### OAuth M2M (Service Principal)

Exchanges a `client_id` + `client_secret` for a short-lived Databricks OAuth token via the `client_credentials` grant on `https://<workspace>/oidc/v1/token`. Tokens are cached per `(workspace, client_id, namespace)` until they are close to expiry.

Credentials are resolved from:

- `client_id` / `client_secret` keys of a k8s secret named `databricks-oauth` (override with `databricks_oauth_secret` or `FLYTE_DATABRICKS_OAUTH_SECRET_NAME`) in the workflow namespace, or
- `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` env vars on the connector.

Operator-side setup (connector deployment):

```yaml
env:
  - name: FLYTE_DATABRICKS_AUTH_TYPE
    value: oauth_m2m
  - name: DATABRICKS_CLIENT_ID
    value: "<service-principal-client-id>"
  - name: DATABRICKS_CLIENT_SECRET
    valueFrom:
      secretKeyRef: { name: databricks-oauth, key: client_secret }
```

No workflow code changes are needed once the connector is reconfigured.

### OIDC Federation

Databricks verifies a JWT signed by a trusted external OIDC provider (e.g. the EKS OIDC issuer for your cluster, or the Kubernetes API server itself) and issues a short-lived Databricks OAuth token. No long-lived client secret is stored anywhere. The connector uses the `urn:ietf:params:oauth:grant-type:token-exchange` grant on `https://<workspace>/oidc/v1/token` with `subject_token_type=urn:ietf:params:oauth:token-type:jwt`.

Two deployment models are supported, **automatically dispatched at submit time**:

- **Model 2 — Per-workflow-namespace ServiceAccount** is selected when the connector finds a labelled+annotated ServiceAccount in the workflow's namespace. Each namespace can federate as a different Databricks Service Principal, giving per-namespace Unity Catalog tenancy.
- **Model 1 — Connector pod identity** is the fallback when no annotated SA is found. All workflows share the connector pod's own Databricks identity.

#### Model 2 — Per-workflow-namespace ServiceAccount (auto-discovered)

Operators opt a workflow namespace into Model 2 by **labelling and annotating a ServiceAccount** in that namespace. The connector lists SAs at submit time, picks the one carrying both markers, mints a JWT for it via the Kubernetes [`TokenRequest`](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.29/#tokenrequest-v1-authentication-k8s-io) API, and exchanges that JWT for a Databricks bearer issued to the Service Principal named in the annotation.

Per-workflow-namespace setup:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: dbx-runner                 # any name - connector discovers it
  namespace: <workflow-namespace>
  labels:
    flyte.org/databricks-enabled: "true"
  annotations:
    flyte.org/databricks-client-id: "<sp-application-id-uuid>"
    flyte.org/databricks-audience: "databricks"   # optional; defaults to "databricks"
```

Connector deployment env (the same one-time config covers every workflow namespace; per-namespace tenancy is driven by the SA annotations, not by per-namespace connector env):

```yaml
env:
  - name: FLYTE_DATABRICKS_AUTH_TYPE
    value: oidc_federation
  # DATABRICKS_CLIENT_ID is *only* needed if you also want a Model 1 fallback
  # for namespaces that have no annotated SA. If unset, missing-SA namespaces fail
  # loudly rather than silently downgrading to a shared identity.
```

Required RBAC on the connector's ServiceAccount (cluster-wide, since the connector serves any workflow namespace):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: databricks-connector-discovery
rules:
  - apiGroups: [""]
    resources: ["serviceaccounts"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["serviceaccounts/token"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: databricks-connector-discovery
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: databricks-connector-discovery
subjects:
  - kind: ServiceAccount
    name: <connector-sa>
    namespace: <connector-namespace>
```

For tighter scoping, swap `ClusterRoleBinding` for per-namespace `RoleBinding`s — each new workflow namespace then needs an explicit binding.

The Databricks Federation Policy on the SP referenced by `flyte.org/databricks-client-id` must list the workflow-namespace SA as the federated subject:

```text
issuer:    <EKS or k8s OIDC issuer URL>
subject:   system:serviceaccount:<workflow-namespace>:<sa-name>
audience:  databricks
```

**Discovery semantics:**

- **Exactly one** annotated SA per namespace → Model 2 with that SA + its annotated `client_id` and `audience`.
- **Zero** annotated SAs → Model 1 fallback, but only if the connector itself has both `DATABRICKS_CLIENT_ID` and a reachable subject token file (e.g. IRSA). Otherwise the create() fails with an actionable `DatabricksAuthError`.
- **Multiple** annotated SAs in the same namespace → hard error listing the conflicting names. Annotate exactly one.
- **Caching:** discovery results (hits and misses) are cached per namespace for 5 minutes. After changing an SA annotation, restart the connector pod for immediate effect; otherwise the change picks up after TTL.

#### Model 1 — Connector pod identity (fallback)

The connector pod's own projected JWT is exchanged. On EKS this is the token at `$AWS_WEB_IDENTITY_TOKEN_FILE` provisioned by IRSA. All workflows share the same Databricks identity — the connector's Service Principal. This is the right mode when you don't need per-namespace UC tenancy.

Required setup:

1. Create a Service Principal in Databricks, grant it workspace access and the permissions your jobs need.
2. Configure a Databricks Federation Policy for that Service Principal pointing at your OIDC issuer (e.g. the EKS cluster OIDC URL) with `subject` claim matching the connector pod's ServiceAccount: `system:serviceaccount:<connector-namespace>:<connector-sa>`.
3. On the connector pod: annotate its ServiceAccount with the IAM role ARN (IRSA) so `AWS_WEB_IDENTITY_TOKEN_FILE` is mounted, and set:

   ```yaml
   env:
     - name: FLYTE_DATABRICKS_AUTH_TYPE
       value: oidc_federation
     - name: DATABRICKS_CLIENT_ID
       value: "<service-principal-client-id>"
   ```

4. Make sure no workflow namespace contains an SA labelled `flyte.org/databricks-enabled=true` — otherwise that namespace will be auto-promoted to Model 2.

The JWT file is re-read on every token refresh, so IRSA rotation is handled automatically.

### Migration guide

All of these paths require only operator-side changes; workflow Python code stays the same unless a single workflow wants to diverge from the connector default.

**PAT → OAuth M2M**

1. Create a Service Principal in Databricks and grant it the same access your PAT had.
2. Store its `client_id` and `client_secret` either in a `databricks-oauth` k8s secret per workflow namespace, or as env vars on the connector.
3. Set `FLYTE_DATABRICKS_AUTH_TYPE=oauth_m2m` on the connector deployment, and redeploy.

**PAT → OIDC Federation (Model 1)**

1. Create a Service Principal in Databricks (no client secret needed).
2. Configure a Federation Policy on that Service Principal that trusts your Kubernetes/EKS OIDC issuer and matches the connector pod's ServiceAccount (by `subject`).
3. Annotate the connector's ServiceAccount with the IAM role (IRSA) if you're on EKS, or otherwise ensure a projected JWT is mounted.
4. Set `FLYTE_DATABRICKS_AUTH_TYPE=oidc_federation` + `DATABRICKS_CLIENT_ID=<sp-client-id>` on the connector, and redeploy.

**PAT → OIDC Federation (Model 2, per-namespace tenancy)**

1. Create a separate Databricks Service Principal **per workflow namespace** that needs its own UC permissions.
2. For each SP, configure a Federation Policy whose `subject` is `system:serviceaccount:<workflow-namespace>:<sa-name>` and `audience` is `databricks`.
3. In each workflow namespace, create a ServiceAccount with these markers:

   ```yaml
   labels:      { flyte.org/databricks-enabled: "true" }
   annotations: { flyte.org/databricks-client-id: "<that-namespace's-sp-uuid>" }
   ```

4. Add the RBAC ClusterRole + ClusterRoleBinding above to the connector's ServiceAccount.
5. Set `FLYTE_DATABRICKS_AUTH_TYPE=oidc_federation` on the connector — that's the only connector-side change. Discovery handles the rest.

No workflow code changes anywhere.

### Backward compatibility

- Jobs created by a pre-OAuth/OIDC connector version store only `auth_token` in their metadata. Newer connectors polling those jobs will use the stored token as-is and skip the refresh-on-401 path, preserving the previous behavior.
- Jobs created by a newer connector and polled by an older connector will also work: the newer connector stores a PAT token in `auth_token` whenever `auth_type == "pat"`, and older connectors ignore the additional metadata fields they don't know about.

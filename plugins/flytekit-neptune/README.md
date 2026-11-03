# Flytekit Neptune Plugin

Neptune is the MLOps stack component for experiment tracking. It offers a single place to log, compare, store, and collaborate on experiments and models. This plugin integrates Flyte with Neptune by configuring links between the two platforms.

To install the plugin, run:

```bash
pip install flytekitplugins-neptune
```

Neptune requires an API key to authenticate with their platform. This Flyte plugin requires a `flytekit` `Secret` to be configured using [Flyte's Secrets manager](https://docs.flyte.org/en/latest/user_guide/productionizing/secrets.html).


```python
from flytekit import Secret, current_context, task
from flytekitplugins.neptune import neptune_scale_run

neptune_api_token = Secret(key="neptune_api_token", group="neptune_group")

@task
@neptune_scale_run(project="flytekit/project", secret=neptune_api_token)
def neptune_task() -> bool:
    ctx = current_context()
    run = ctx.neptune_run
    run.log_configs({"parameters/batch_size": 64})
    ...
```

To enable linking from the Flyte side panel to Neptune, add the following to Flyte's configuration:

```yaml
plugins:
  logs:
    dynamic-log-links:
      - neptune-scale-experiment:
          displayName: Neptune Experiment
          templateUris:
            - "https://scale.neptune.ai/{{ .taskConfig.project }}/runs/details?viewId=standard-view&runIdentificationKey={{ .taskConfig.id }}&type=experiment"
      - neptune-scale-run:
          displayName: Neptune Run
          templateUris:
            - "https://scale.neptune.ai/{{ .taskConfig.project }}/-/run/?customId={{ .podName }}"
      - neptune-scale-custom-id:
          displayName: Neptune Run
          templateUris:
            - "https://scale.neptune.ai/{{`{{ .taskConfig.project }}`}}/-/run/?customId={{`{{ .taskConfig.id }}`}}"
```

To use Neptune 2.x (the older version), install `flytekitplugins-neptune[legacy]`.

```python
from flytekit import Secret, current_context, task
from flytekitplugins.neptune import neptune_init_run

neptune_api_token = Secret(key="neptune_api_token", group="neptune_group")

@task
@neptune_init_run(project="flytekit/project", secret=neptune_api_token)
def neptune_task() -> bool:
    ctx = current_context()
    run = ctx.neptune_run
    run["algorithm"] = "my_algorithm"
    ...
```

To enable linking from the Flyte side panel to Neptune, add the following to Flyte's configuration:

```yaml
plugins:
  logs:
    dynamic-log-links:
      - neptune-run-id:
          displayName: Neptune
          templateUris: "{{ .taskConfig.host }}/{{ .taskConfig.project }}?query=(%60flyte%2Fexecution_id%60%3Astring%20%3D%20%22{{ .executionName }}-{{ .nodeId }}-{{ .taskRetryAttempt }}%22)&lbViewUnpacked=true"
```

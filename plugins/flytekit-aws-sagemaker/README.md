# AWS SageMaker Plugin

The plugin features connectors for SageMaker deployment, model training,
processing, hyperparameter tuning, batch inference (a.k.a. batch transform),
and inference recommendations.

## Inference

The deployment connector enables you to deploy models, create and trigger inference endpoints.
Additionally, you can entirely remove the SageMaker deployment using the `delete_sagemaker_deployment` workflow.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-awssagemaker
```

Here is a sample SageMaker deployment workflow:

```python
from flytekitplugins.awssagemaker_inference import create_sagemaker_deployment


REGION = os.getenv("REGION")
MODEL_NAME = "xgboost"
ENDPOINT_CONFIG_NAME = "xgboost-endpoint-config"
ENDPOINT_NAME = "xgboost-endpoint"

sagemaker_deployment_wf = create_sagemaker_deployment(
    name="sagemaker-deployment",
    model_input_types=kwtypes(model_path=str, execution_role_arn=str),
    model_config={
        "ModelName": MODEL_NAME,
        "PrimaryContainer": {
            "Image": "{images.deployment_image}",
            "ModelDataUrl": "{inputs.model_path}",
        },
        "ExecutionRoleArn": "{inputs.execution_role_arn}",
    },
    endpoint_config_input_types=kwtypes(instance_type=str),
    endpoint_config_config={
        "EndpointConfigName": ENDPOINT_CONFIG_NAME,
        "ProductionVariants": [
            {
                "VariantName": "variant-name-1",
                "ModelName": MODEL_NAME,
                "InitialInstanceCount": 1,
                "InstanceType": "{inputs.instance_type}",
            },
        ],
        "AsyncInferenceConfig": {
            "OutputConfig": {"S3OutputPath": os.getenv("S3_OUTPUT_PATH")}
        },
    },
    endpoint_config={
        "EndpointName": ENDPOINT_NAME,
        "EndpointConfigName": ENDPOINT_CONFIG_NAME,
    },
    images={"deployment_image": custom_image},
    region=REGION,
)


@workflow
def model_deployment_workflow(
    model_path: str = os.getenv("MODEL_DATA_URL"),
    execution_role_arn: str = os.getenv("EXECUTION_ROLE_ARN"),
) -> str:
    return sagemaker_deployment_wf(
        model_path=model_path,
        execution_role_arn=execution_role_arn,
        instance_type="ml.m4.xlarge",
    )
```

## Training

`SageMakerTrainingJobTask` runs a `CreateTrainingJob` and waits for it to reach a
terminal state. The describe-poll loop runs server-side via the connector; no
Flyte worker holds a session open for the training duration. While running, the
task surfaces SageMaker's `SecondaryStatus` (`Starting`, `Downloading`,
`Training`, `Uploading`, …) as the live message. On success it emits a single
`result: dict` literal with:

- `TrainingJobArn`, `TrainingJobName`
- `ModelArtifacts.S3ModelArtifacts` — the S3 URI of the trained `model.tar.gz`
- `OutputDataConfig.S3OutputPath` — sibling location for checkpoints / TensorBoard
- `FinalMetricDataList` — last value of every metric defined in `MetricDefinitions`
- `BillableTimeInSeconds`, `TrainingTimeInSeconds`

```python
from flytekitplugins.awssagemaker_training import SageMakerTrainingJobTask
from flytekit import kwtypes, workflow

training = SageMakerTrainingJobTask(
    name="train-xgboost",
    config={
        "TrainingJobName": "xgb-{idempotence_token}",
        "AlgorithmSpecification": {
            "TrainingImage": "{images.training_image}",
            "TrainingInputMode": "File",
            "MetricDefinitions": [
                {"Name": "validation:auc", "Regex": "auc=([0-9\\.]+)"},
            ],
        },
        "RoleArn": "{inputs.execution_role_arn}",
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "{inputs.train_data}",
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
            }
        ],
        "OutputDataConfig": {"S3OutputPath": "{inputs.output_prefix}"},
        "ResourceConfig": {
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 30,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
    },
    region="<aws-region>",
    images={"training_image": "<your-ecr-uri-or-ImageSpec>"},
    inputs=kwtypes(execution_role_arn=str, train_data=str, output_prefix=str),
)
```

A training job writes `model.tar.gz` to S3 but does **not** create a SageMaker
`Model` entity. Chain a `SageMakerModelTask` downstream, feeding it
`result["ModelArtifacts"]["S3ModelArtifacts"]` as `PrimaryContainer.ModelDataUrl`,
to deploy the trained artefact via an endpoint or a batch-transform job.

Registering the artifact with SageMaker Model Registry is a separate
`CreateModelPackage` operation and is not performed by this task.

Inputs are S3-resident. To use a Glue/Athena-backed dataset, either pass the
underlying S3 location of the Glue table directly, or stage query results to S3
with an upstream Flyte task and pass that S3 URI in.

## Processing

`SageMakerProcessingJobTask` runs a `CreateProcessingJob` and waits for it to
reach a terminal state, using the same server-side describe-poll loop as the
training task. Processing jobs cover the steps that bookend training — feature
engineering / data cleaning (pre-training), and model evaluation, batch scoring
with custom pre/post-processing, or SageMaker Clarify bias & explainability
(post-training) — on managed SageMaker infra under the same execution role.

Unlike training, the container image lives at `AppSpecification.ImageUri`.
Inputs are commonly S3-resident, while `ProcessingOutputConfig` can write to S3
or SageMaker Feature Store. Processing jobs expose no `SecondaryStatus`, so the
live message is empty while running; on failure the task surfaces
`FailureReason` (falling back to `ExitMessage`). On success it emits a single
`result: dict` literal with:

- `ProcessingJobArn`, `ProcessingJobName`
- `Outputs` — a list projected from `ProcessingOutputConfig.Outputs`. Each
  item contains `OutputName` plus either `S3Uri` for an S3 destination or
  `FeatureGroupName` for a Feature Store destination.
- `ExitMessage`, `ProcessingStartTime`, `ProcessingEndTime`

```python
from flytekitplugins.awssagemaker_processing import SageMakerProcessingJobTask
from flytekit import kwtypes

preprocess = SageMakerProcessingJobTask(
    name="preprocess-features",
    config={
        "ProcessingJobName": "prep-{idempotence_token}",
        "AppSpecification": {
            "ImageUri": "{images.processing_image}",
            "ContainerEntrypoint": ["python3", "/opt/ml/processing/preprocess.py"],
        },
        "RoleArn": "{inputs.execution_role_arn}",
        "ProcessingInputs": [
            {
                "InputName": "raw",
                "S3Input": {
                    "S3Uri": "{inputs.raw_data}",
                    "LocalPath": "/opt/ml/processing/input",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File",
                },
            }
        ],
        "ProcessingOutputConfig": {
            "Outputs": [
                {
                    "OutputName": "train",
                    "S3Output": {
                        "S3Uri": "{inputs.output_prefix}",
                        "LocalPath": "/opt/ml/processing/output",
                        "S3UploadMode": "EndOfJob",
                    },
                }
            ]
        },
        "ProcessingResources": {
            "ClusterConfig": {
                "InstanceType": "ml.m5.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 30,
            }
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
    },
    region="<aws-region>",
    images={"processing_image": "<your-ecr-uri-or-ImageSpec>"},
    inputs=kwtypes(execution_role_arn=str, raw_data=str, output_prefix=str),
)
```

Chain it before a `SageMakerTrainingJobTask` (feed an output's `S3Uri` in as the
training `InputDataConfig` S3 URI) or after one for evaluation. The
`SageMakerStopProcessingJobTask` / `SageMakerDescribeProcessingJobTask` helpers
mirror their training-job counterparts.

## Hyperparameter Tuning

`SageMakerHyperParameterTuningJobTask` runs `CreateHyperParameterTuningJob` and
waits for it to reach a terminal state. The polling loop is identical in shape
to `SageMakerTrainingJobTask`, but each trial is a child training job — so while
running, the task's message field surfaces a compact trial counter
(`"3 Completed / 1 InProgress / 0 Failed trials"`) instead of a single job's
`SecondaryStatus`.

On completion the task emits a single `result: dict` literal with:

- `HyperParameterTuningJobArn`, `HyperParameterTuningJobName`
- `BestTrainingJob` — the winning trial. Contains `TrainingJobName`,
  `TrainingJobArn`, `TunedHyperParameters`, `ObjectiveStatus`,
  `FinalHyperParameterTuningJobObjectiveMetric.{MetricName, Value}` and —
  crucially — `ModelArtifacts.S3ModelArtifacts`. SageMaker's
  `DescribeHyperParameterTuningJob` response does *not* include the trained
  model URI; the connector resolves it via a single follow-up
  `describe_training_job` call so this output chains directly into
  `SageMakerModelTask`.
- `ModelArtifacts.S3ModelArtifacts` — top-level convenience copy of the best
  trial's model URI so the result dict is **shape-compatible with
  `SageMakerTrainingJobTask`'s output**. Any downstream task that reads
  `result["ModelArtifacts"]["S3ModelArtifacts"]` works against either task
  unchanged.
- `TrainingJobStatusCounters` — `Completed` / `InProgress` / `RetryableError`
  / `NonRetryableError` / `Stopped` counts across all trials.
- `ObjectiveStatusCounters` — `Succeeded` / `Pending` / `Failed`. Note these
  count objective-metric *evaluation*, not trial completion. A trial can
  Complete but fail to emit the configured objective metric, in which case it
  lands in `ObjectiveStatusCounters.Failed`.

```python
from flytekitplugins.awssagemaker_hyperparameter_tuning import (
    SageMakerHyperParameterTuningJobTask,
)
from flytekit import kwtypes

tuning = SageMakerHyperParameterTuningJobTask(
    name="tune-xgboost",
    config={
        "HyperParameterTuningJobName": "xgb-tune-{idempotence_token}",
        "HyperParameterTuningJobConfig": {
            "Strategy": "Bayesian",          # Bayesian | Random | Hyperband | Grid
            "HyperParameterTuningJobObjective": {
                "Type": "Minimize",
                "MetricName": "validation:rmse",
            },
            "ResourceLimits": {
                "MaxNumberOfTrainingJobs": 20,
                "MaxParallelTrainingJobs": 4,
            },
            "ParameterRanges": {
                "ContinuousParameterRanges": [
                    {"Name": "eta", "MinValue": "0.01", "MaxValue": "0.5",
                     "ScalingType": "Logarithmic"},
                ],
                "IntegerParameterRanges": [
                    {"Name": "max_depth", "MinValue": "3", "MaxValue": "9"},
                    {"Name": "num_round", "MinValue": "10", "MaxValue": "200"},
                ],
            },
            "TrainingJobEarlyStoppingType": "Auto",
        },
        "TrainingJobDefinition": {
            "AlgorithmSpecification": {
                "TrainingImage": "{images.training_image}",
                "TrainingInputMode": "File",
            },
            "RoleArn": "{inputs.execution_role_arn}",
            "StaticHyperParameters": {"objective": "reg:squarederror"},
            "InputDataConfig": [
                {"ChannelName": "train",      "DataSource": {...}},
                {"ChannelName": "validation", "DataSource": {...}},  # required to emit validation:rmse
            ],
            "OutputDataConfig": {"S3OutputPath": "{inputs.output_prefix}"},
            "ResourceConfig": {"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 30},
            "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
        },
    },
    region="<aws-region>",
    images={"training_image": "<your-ecr-uri-or-ImageSpec>"},
    inputs=kwtypes(execution_role_arn=str, output_prefix=str),
)
```

A few important notes:

- **Cost.** A tuning job's total cost is roughly `MaxNumberOfTrainingJobs ×
  per-trial instance-hours`. Start with a small `ResourceLimits` (4-8 trials)
  while iterating on ranges, then scale up.
- **Objective metric must be emitted.** For SageMaker built-in algorithms
  (XGBoost, BlazingText, etc.) the supported metric names are predefined
  (`validation:rmse`, `validation:auc`, …) and require the corresponding
  channel (e.g. a `validation` `InputDataConfig` channel) — set up the
  channels accordingly. For custom containers, define
  `AlgorithmSpecification.MetricDefinitions` with a regex that matches your
  container's stdout/stderr so SageMaker can scrape the metric out.
- **`Strategy: "Hyperband"`** only works with iterative algorithms that emit
  intermediate objective values, since Hyperband prunes weak trials early.
  Default Bayesian is the safest pick if you're not sure.

To chain HPO directly into the rest of the pipeline, just consume
`result["ModelArtifacts"]["S3ModelArtifacts"]` the same way you would with a
training-job result:

```python
@workflow
def tune_and_deploy() -> dict:
    hpo_result = tuning(...)
    model_result, _ = model_task(
        model_data=hpo_result["ModelArtifacts"]["S3ModelArtifacts"]
    )
    return model_result
```

These tasks can be composed as HPO → model → Inference Recommender → batch
transform, passing the best model artifact and recommended instance type through
normal Flyte task outputs.

Helper sync tasks: `SageMakerStopHyperParameterTuningJobTask` and
`SageMakerDescribeHyperParameterTuningJobTask` follow the stop/describe pattern
from the training and batch-transform connectors.

## Batch Transform (Batch Inference)

`SageMakerTransformJobTask` runs `CreateTransformJob` for offline scoring of a
dataset stored on S3 against an existing SageMaker `Model`. SageMaker writes one
`<input>.out` per input object under `TransformOutput.S3OutputPath`. The task
emits a `result: dict` containing `TransformJobArn`, `TransformJobName`,
`ModelName`, `TransformOutput.S3OutputPath`, `TransformStartTime` and
`TransformEndTime`.

For tabular predictive workloads, set `DataProcessing.JoinSource: "Input"` so
each output line carries the original input columns alongside the prediction —
otherwise the predictions have no key to join back to the source rows.

```python
from flytekitplugins.awssagemaker_batch_transform import SageMakerTransformJobTask
from flytekit import kwtypes

batch_score = SageMakerTransformJobTask(
    name="batch-score",
    config={
        "TransformJobName": "score-{idempotence_token}",
        "ModelName": "{inputs.model_name}",
        "TransformInput": {
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": "{inputs.input_data}",
                }
            },
            "ContentType": "text/csv",
            "SplitType": "Line",
        },
        "TransformOutput": {
            "S3OutputPath": "{inputs.output_prefix}",
            "AssembleWith": "Line",
        },
        "TransformResources": {"InstanceType": "ml.m5.xlarge", "InstanceCount": 1},
        "BatchStrategy": "MultiRecord",
        "DataProcessing": {"JoinSource": "Input"},
    },
    region="<aws-region>",
    inputs=kwtypes(model_name=str, input_data=str, output_prefix=str),
)
```

`ModelName` must reference an existing SageMaker `Model` — typically created
upstream by a `SageMakerModelTask` consuming a training job's
`S3ModelArtifacts` output.

## Inference Recommender

`SageMakerInferenceRecommenderJobTask` runs `CreateInferenceRecommendationsJob`
and waits for it to reach a terminal state. SageMaker benchmarks the model
across several real or candidate instance types and returns a ranked list of
`InferenceRecommendations`. The task emits a single `result: dict` containing:

- `JobArn`, `JobName`, `JobType` (`Default` or `Advanced`)
- `InferenceRecommendations` — ranked list. Each entry has
  `EndpointConfiguration.InstanceType`, `InitialInstanceCount`, optional
  `ServerlessConfig`, plus `Metrics` (`CostPerHour`, `CostPerInference`,
  `MaxInvocations`, `ModelLatency`, `CpuUtilization`, `MemoryUtilization`,
  `ModelSetupTime`) and `ModelConfiguration`.
- `EndpointPerformances` — populated for `Default` jobs that benchmark existing
  endpoints supplied through `InputConfig.Endpoints`.
- `CompletionTime`

Two input modes are supported by SageMaker:

- `ModelPackageVersionArn` — point at a versioned entry in a Model Package Group.
- `ModelName` + `ContainerConfig` — point at a bare `SageMaker.Model` plus a
  payload archive and framework hint. Easier to chain after a fresh
  `SageMakerTrainingJobTask`/`SageMakerModelTask` because no model-package
  registration is required.

`ContainerConfig.PayloadConfig.SamplePayloadUrl` must be an S3 URL to a single
`.tar.gz` archive containing the sample request body the Recommender will use
when benchmarking. `SupportedInstanceTypes` constrains the sweep to a fixed
list (omit it for a full sweep of the framework's supported instances).

#### Default vs Advanced jobs — what fields each accepts

The `CreateInferenceRecommendationsJob` boto3 API exposes a lot of bounds
fields under both `InputConfig` and the top level regardless of `JobType`, but
**AWS only accepts most of them when `JobType="Advanced"`** and returns
`ValidationException` if you set them on a `Default` job. Default jobs are
essentially fire-and-forget for ~45 minutes; the only effective bound is
`ContainerConfig.SupportedInstanceTypes`.

| Field | `Default` | `Advanced` |
|---|---|---|
| `InputConfig.ModelPackageVersionArn` *or* `ModelName` + `ContainerConfig` | required | required |
| `InputConfig.ContainerConfig.SupportedInstanceTypes` | **the only bound** | optional |
| `InputConfig.JobDurationInSeconds` | rejected | required |
| `InputConfig.TrafficPattern` | rejected | required |
| `InputConfig.ResourceLimit` | rejected | required |
| `InputConfig.EndpointConfigurations` | rejected | required |
| top-level `StoppingConditions` | rejected | optional |
| `OutputConfig` | optional | optional |

```python
from flytekitplugins.awssagemaker_inference_recommender import (
    SageMakerInferenceRecommenderJobTask,
)
from flytekit import kwtypes

# Default job — fire-and-forget instance recommendation across the listed
# SupportedInstanceTypes. No StoppingConditions / JobDurationInSeconds.
recommend = SageMakerInferenceRecommenderJobTask(
    name="recommend-instance",
    config={
        "JobName": "rec-{idempotence_token}",
        "JobType": "Default",
        "RoleArn": "{inputs.execution_role_arn}",
        "InputConfig": {
            "ModelName": "{inputs.model_name}",
            "ContainerConfig": {
                "Domain": "MACHINE_LEARNING",
                "Task": "OTHER",
                "Framework": "XGBOOST",
                "FrameworkVersion": "1.7",
                "PayloadConfig": {
                    "SamplePayloadUrl": "{inputs.payload_url}",
                    "SupportedContentTypes": ["text/csv"],
                },
                "SupportedInstanceTypes": [
                    "ml.m5.large",
                    "ml.m5.xlarge",
                    "ml.c5.large",
                    "ml.c5.xlarge",
                ],
            },
        },
    },
    region="<aws-region>",
    inputs=kwtypes(execution_role_arn=str, model_name=str, payload_url=str),
)
```

For an `Advanced` load test, the same task class accepts the full set of
fields the Default config rejects:

```python
from flytekit import kwtypes

recommend_advanced = SageMakerInferenceRecommenderJobTask(
    name="recommend-load-test",
    config={
        "JobName": "rec-adv-{idempotence_token}",
        "JobType": "Advanced",
        "RoleArn": "{inputs.execution_role_arn}",
        "InputConfig": {
            "ModelName": "{inputs.model_name}",
            "ContainerConfig": {...},                # same as above
            "JobDurationInSeconds": 7200,            # Advanced-only
            "TrafficPattern": {                      # Advanced-only
                "TrafficType": "PHASES",
                "Phases": [
                    {"InitialNumberOfUsers": 1, "SpawnRate": 1, "DurationInSeconds": 120},
                ],
            },
            "ResourceLimit": {                       # Advanced-only
                "MaxNumberOfTests": 10,
                "MaxParallelOfTests": 2,
            },
            "EndpointConfigurations": [              # Advanced-only
                {"InstanceType": "ml.m5.xlarge"},
                {"InstanceType": "ml.c5.xlarge"},
            ],
        },
        "StoppingConditions": {                      # top-level, Advanced-only
            "MaxInvocations": 500,
            "ModelLatencyThresholds": [
                {"Percentile": "P95", "ValueInMilliseconds": 500},
            ],
        },
    },
    region="<aws-region>",
    inputs=kwtypes(execution_role_arn=str, model_name=str, payload_url=str),
)
```

### End-to-end: train, recommend, then batch transform on the recommended instance

The recommender's `result["InferenceRecommendations"][0]["EndpointConfiguration"]["InstanceType"]`
is a stable scalar — pull it out in a small `@task` and feed it directly to the
next SageMaker task as a Flyte Promise. The boto3 mixin substitutes `{inputs.X}`
placeholders into the config at runtime, so the recommended instance type lands
in `TransformResources.InstanceType` (or `ProductionVariants[*].InstanceType`)
without any extra plumbing.

```python
from flytekit import kwtypes, task, workflow
from flytekitplugins.awssagemaker_batch_transform import SageMakerTransformJobTask
from flytekitplugins.awssagemaker_inference import SageMakerModelTask
from flytekitplugins.awssagemaker_inference_recommender import (
    SageMakerInferenceRecommenderJobTask,
)
from flytekitplugins.awssagemaker_training import SageMakerTrainingJobTask


@task
def top_instance_type(recommender_result: dict) -> str:
    """Pick the cheapest-meets-SLA instance the Recommender returned."""
    return recommender_result["InferenceRecommendations"][0]["EndpointConfiguration"]["InstanceType"]


training = SageMakerTrainingJobTask(...)            # see Training section
model    = SageMakerModelTask(...)                  # wraps S3ModelArtifacts as a Model
recommend = SageMakerInferenceRecommenderJobTask(...)  # see snippet above
batch_score = SageMakerTransformJobTask(
    name="batch-score-recommended",
    config={
        "TransformJobName": "score-{idempotence_token}",
        "ModelName": "{inputs.model_name}",
        "TransformInput": {...},
        "TransformOutput": {"S3OutputPath": "{inputs.output_prefix}"},
        "TransformResources": {
            # Recommender's pick flows in here via the Flyte Promise wired up below.
            "InstanceType": "{inputs.instance_type}",
            "InstanceCount": 1,
        },
    },
    region="<aws-region>",
    inputs=kwtypes(model_name=str, instance_type=str, output_prefix=str),
)


@workflow
def train_recommend_transform() -> dict:
    train_result = training(...)
    model_result, _ = model(
        model_data=train_result["ModelArtifacts"]["S3ModelArtifacts"]
    )
    model_name = model_result["ModelArn"].rsplit("/", 1)[-1]

    rec_result = recommend(model_name=model_name)
    instance_type = top_instance_type(recommender_result=rec_result)

    return batch_score(
        model_name=model_name,
        instance_type=instance_type,
        output_prefix="s3://<bucket>/predictions/",
    )
```

The same pattern composes into an end-to-end training → model → recommender →
batch-transform workflow using normal Flyte task outputs.

Helper sync tasks: `SageMakerStopInferenceRecommenderJobTask` and
`SageMakerDescribeInferenceRecommenderJobTask` mirror the stop/describe pattern
from the training and batch-transform connectors and are useful for inspecting
historical recommender runs from a Flyte workflow.

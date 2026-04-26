# Flytekit AWS EMR Serverless Plugin

A Flyte connector for [AWS EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html) that submits Spark and Hive jobs to an EMR Serverless application and tracks them through to completion.

## Features

- **Pythonic Spark mode**: write a Flyte `@task` whose body is regular PySpark; the plugin packages the user code, uploads it to S3, and runs it on EMR Serverless. No long-lived cluster to manage.
- **Script Spark mode**: point at an existing `main.py` (or JAR) already in S3 and submit it directly.
- **Hive mode**: submit a Hive query (inline or from S3) against an EMR Serverless application configured for Hive.
- Async connector lifecycle (`create` / `get` / `delete`) so the connector pod stays light and many jobs can be tracked concurrently.
- Honours Flyte task retries, timeouts, and cancellation, and surfaces EMR Serverless logs through the Flyte UI when log URIs are available.

## Installation

```bash
pip install flytekitplugins-awsemrserverless
```

The connector is registered automatically with `flytekit` via the plugin entry point. Deploy it on a [`flyteconnector`](https://github.com/flyteorg/flyte/tree/master/charts/flyteconnector) pod that has this package installed and an IAM identity allowed to call EMR Serverless `StartJobRun` / `GetJobRun` / `CancelJobRun` and to read/write the script-staging S3 prefix.

## Usage

### Pythonic Spark task

```python
from flytekit import task, workflow
from flytekitplugins.awsemrserverless import EMRServerless, EMRServerlessSparkJobDriver


@task(
    task_config=EMRServerless(
        application_id="00fhabc12345",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessRole",
        region="us-east-1",
        job_driver=EMRServerlessSparkJobDriver(
            spark_submit_parameters="--conf spark.executor.cores=2 --conf spark.executor.memory=4g",
        ),
    ),
)
def spark_count() -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return spark.range(1_000_000).count()


@workflow
def wf() -> int:
    return spark_count()
```

The plugin serializes the task body, uploads it to S3, and EMR Serverless runs it inside the worker image you have associated with the application.

### Script Spark task

```python
@task(
    task_config=EMRServerless(
        application_id="00fhabc12345",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessRole",
        region="us-east-1",
        job_driver=EMRServerlessSparkJobDriver(
            entry_point="s3://my-bucket/scripts/main.py",
            entry_point_arguments=["--date", "2025-01-01"],
            spark_submit_parameters="--conf spark.executor.memory=4g",
        ),
    ),
)
def submit_script():
    ...
```

### Hive task

```python
from flytekitplugins.awsemrserverless import EMRServerless, EMRServerlessHiveJobDriver


@task(
    task_config=EMRServerless(
        application_id="00fhabc12345",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessRole",
        region="us-east-1",
        job_driver=EMRServerlessHiveJobDriver(
            query="SELECT COUNT(*) FROM my_table",
        ),
    ),
)
def hive_query():
    ...
```

## Worker image

For Pythonic Spark tasks the worker image must contain `flytekit` and this plugin so the executor can rehydrate the task object on the EMR Serverless side. Script Spark and Hive jobs do not require flytekit on the worker.

A reference `Dockerfile` is shipped alongside this plugin; it builds on the public EMR Serverless Spark base image and installs the matching `flytekit` and `flytekitplugins-awsemrserverless` versions:

```bash
docker build \
  --build-arg VERSION=<flytekit-version> \
  -t <registry>/emr-serverless-flytekit:<tag> \
  plugins/flytekit-aws-emr-serverless
```

Override the base with `--build-arg EMR_BASE_IMAGE=...` to track a different EMR release.

## IAM

The connector pod's IAM principal needs:

- `emr-serverless:StartJobRun`, `emr-serverless:GetJobRun`, `emr-serverless:CancelJobRun` on the target application
- `s3:GetObject` / `s3:PutObject` on the script-staging prefix (Pythonic mode)
- `iam:PassRole` for the EMR Serverless execution role

The execution role attached to the EMR Serverless application is the role the workers run as and needs whatever data-access permissions your jobs require.

## Discussion

Tracking issue: [flyteorg/flyte#7286](https://github.com/flyteorg/flyte/issues/7286).

# Slurm Agent Demo

In this guide, we will briefly introduce how to setup an environment to test Slurm agent locally without running the backend service (e.g., flyte agent gRPC server).

## Table of Content
* [Overview](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#overview)
* [Setup a Local Environment](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#setup-a-local-environment)
    * [Flyte Client (Localhost)](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#flyte-client-localhost)
    * [Remote Tiny Slurm Cluster](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#remote-tiny-slurm-cluster)
    * [Amazon S3 Bucket](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#amazon-s3-bucket)
* [Run a Demo](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#run-a-demo)
* [Result](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#result)

## Overview
Slurm agent on the highest level has three core methods to interact with a Slurm cluster:
1. `create`: Use `srun` to run a Slurm job which executes Flyte entrypoints, `pyflyte-fast-execute` and `pyflyte-execute`
2. `get`: Use `scontrol show job <job_id>` to monitor the Slurm job state
3. `delete`: Use `scancel <job_id>` to cancel the Slurm job (this method is still under test)

The following figure illustrates the interaction between a client and a remote Slurm cluster. To facilitate file access between two machines, such as flyte task input and output `.pb` files and task modules, we use an Amazon S3 bucket for data storage.

![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/overview_v2.png)

## Setup a Local Environment
Without running the backend service, we can setup an environment to test the Slurm agent locally. The setup is divided into three components: a client (localhost), a remote tiny Slurm cluster, and an Amazon S3 bucket that facilitates communication between the two.

### Flyte Client (Localhost)
1. Setup a local Flyte cluster following this [official guide](https://docs.flyte.org/en/latest/community/contribute/contribute_code.html#how-to-setup-dev-environment-for-flytekit)
2. Build a virtual environment (e.g., conda) and activate it
3. Clone Flytekit repo, checkout the Slurm agent PR, and install Flytekit
```
git clone https://github.com/flyteorg/flytekit.git
gh pr checkout 3005
make setup && pip install -e .
```
4. Install Flytekit Slurm agent
```
cd plugins/flytekit-slurm/
pip install -e .
```

### Remote Tiny Slurm Cluster
To simplify the setup process, we follow this [guide](https://github.com/JiangJiaWei1103/Slurm-101) to configure a single-host Slurm cluster, covering `slurmctld` (the central management daemon) and `slurmd` (the compute node daemon).

After building a Slurm cluster, we need to install Flytekit similar to the Flyte client.
1. Setup a local Flyte cluster following this [official guide](https://docs.flyte.org/en/latest/community/contribute/contribute_code.html#how-to-setup-dev-environment-for-flytekit)
2. Build a conda environment called `dev` and activate it
3. Clone Flytekit repo and install Flytekit
```
git clone https://github.com/flyteorg/flytekit.git
make setup && pip install -e .
```

### Amazon S3 Bucket
1. Click "Create bucket" button tp create a bucket on this [page](https://us-west-2.console.aws.amazon.com/s3/get-started?region=us-west-2&bucketType=general)
    * Give the cluster an unique name and leave other settings as default
2. Click the user on the top right corner and go to "Security credentials"
3. Create an access key and save it
4. Configure AWS access on both machines
```
# ~/.aws/config
[default]
region=<your_region>

# ~/.aws/credentials
[default]
aws_access_key_id=<aws_access_key_id>
aws_secret_access_key=<aws_secret_access_key>
```

Now, both machines have access to the Amazon S3 bucket.

## Run a Demo
We use the following script to test the Slurm agent on the client side.

```python
# demo_slurm.py
import os
from typing import Any, Dict

from flytekit import kwtypes, task, workflow, ImageSpec
from flytekitplugins.slurm import Slurm, SlurmTask


@task(
    task_config=Slurm(
        srun_conf={
            "partition": "debug",
            "job-name": "demo-slurm",
            # Remote working directory
            "chdir": "<your_remote_working_dir>"
        }
    )
)
def plus_one(x: int) -> int:
    return x + 1


@task
def greet(year: int) -> str:
    return f"Hello {year}!!!"


@workflow
def wf(x: int) -> str:
    """Return plus one result now.

    Return slurm job information?
    """
    x = plus_one(x=x)
    msg = greet(year=x)

    return msg


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)

    # Local run
    print(f">>> LOCAL EXEC <<<")
    result = runner.invoke(pyflyte.main, ["run", "--raw-output-data-prefix", "<your_s3_bucket_uri>", path, "wf", "--x", 2024])
    print(result.output)
```

We expect "Hello 2025!!!" as the output on the Flyte client terminal.

## Result
### Flyte Client
![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/flyte_client.png)

### Remote Tiny Slurm Cluster
![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/remote_tiny_slurm_cluster.png)

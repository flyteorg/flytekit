# Slurm Agent Demo

> Note: This document is still a work in progress, focusing on demonstrating the initial implementation. It will be updated and refined frequently until a stable version is ready.

In this guide, we will briefly introduce how to setup an environment to test Slurm agent locally without running the backend service (e.g., flyte agent gRPC server). It covers both basic and advanced use cases: the basic use case involves executing a shell script directly, while the advanced use case enables running user-defined functions on a Slurm cluster.

## Table of Content
* [Overview](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#overview)
* [Setup a Local Test Environment](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#setup-a-local-test-environment)
    * [Flyte Client (Localhost)](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#flyte-client-localhost)
    * [Remote Tiny Slurm Cluster](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#remote-tiny-slurm-cluster)
    * [SSH Configuration](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#ssh-configuration)
    * [(Optional) Setup Amazon S3 Bucket](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#optional-setup-amazon-s3-bucket)
* [Rich Use Cases](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#rich-use-cases)
    * [`SlurmTask`](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#slurmtask)
    * [`SlurmShellTask`](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#slurmshelltask)
    * [`SlurmFunctionTask`](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#slurmfunctiontask)

## Overview
Slurm agent on the highest level has three core methods to interact with a Slurm cluster:
1. `create`: Use `srun` or `sbatch` to run a job on a Slurm cluster
2. `get`: Use `scontrol show job <job_id>` to monitor the Slurm job state
3. `delete`: Use `scancel <job_id>` to cancel the Slurm job (this method is still under test)

In the simplest form, Slurm agent supports directly running a batch script using `sbatch` on a Slurm cluster as shown below:

![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/basic_arch.png)

## Setup a Local Test Environment
Without running the backend service, we can setup an environment to test Slurm agent locally. The setup consists of two main components: a client (localhost) and a remote tiny Slurm cluster. Then, we need to configure SSH connection to facilitate communication between the two, which relies on `asyncssh`. Additionally, an S3-compatible object storage is needed for advanced use cases and we choose [Amazon S3](https://us-west-2.console.aws.amazon.com/s3/get-started?region=us-west-2&bucketType=general) for demonstration here.
> Note: A persistence layer (such as S3-compatible object storage) becomes essential as scenarios grow more complex, especially when integrating heterogeneous task types into a workflow in the future.

### Flyte Client (Localhost)
1. Setup a local Flyte cluster following this [official guide](https://docs.flyte.org/en/latest/community/contribute/contribute_code.html#how-to-setup-dev-environment-for-flytekit)
2. Build a virtual environment (e.g., [poetry](https://python-poetry.org/), [conda](https://docs.conda.io/en/latest/)) and activate it
3. Clone Flytekit [repo](https://github.com/flyteorg/flytekit), checkout the Slurm agent [PR](https://github.com/flyteorg/flytekit/pull/3005/), and install Flytekit
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

After building a Slurm cluster, we need to install Flytekit and Slurm agent, just as what we did in the previous [section](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#flyte-client-localhost).
1. Build a virtual environment and activate it (we take `poetry` as an example):
```
poetry new demo-env

# For running a subshell with the virtual environment activated
poetry self add poetry-plugin-shell

# Activate the virtual environment
poetry shell
```
2. Clone Flytekit [repo](https://github.com/flyteorg/flytekit), checkout the Slurm agent [PR](https://github.com/flyteorg/flytekit/pull/3005/), and install Flytekit
```
git clone https://github.com/flyteorg/flytekit.git
gh pr checkout 3005
make setup && pip install -e .
```
3. Install Flytekit Slurm agent
```
cd plugins/flytekit-slurm/
pip install -e .
```

### SSH Configuration
To facilitate communication between the Flyte client and the remote Slurm cluster, we setup SSH on the Flyte client side as follows:
1. Create a new authentication key pair
```
ssh-keygen -t rsa -b 4096
```
2. Copy the public key into the remote Slurm cluster
```
ssh-copy-id <username>@<remote_server_ip>
```
3. Enable key-based authentication
```
# ~/.ssh/config
Host <host_alias>
  HostName <remote_server_ip>
  Port <ssh_port>
  User <username>
  IdentityFile <path_to_private_key>
```
Then, run a sanity check to make sure we can connect to the Slurm cluster:
```
ssh <host_alias>
```
Simple and elegant!

### (Optional) Setup Amazon S3 Bucket
For those interested in advanced use cases, in which user-defined functions are sent and executed on the Slurm cluster, an S3-compitable object storage becomes a necessary component. Following summarizes the setup process:
1. Click "Create bucket" button (marked in yellow) to create a bucket on this [page](https://us-west-2.console.aws.amazon.com/s3/get-started?region=us-west-2&bucketType=general)
    * Give the cluster an unique name and leave other settings as default
2. Click the user on the top right corner and go to "Security credentials"
3. Create an access key and save it
4. Configure AWS access on **both** machines
```
# ~/.aws/config
[default]
region=<your_region>

# ~/.aws/credentials
[default]
aws_access_key_id=<aws_access_key_id>
aws_secret_access_key=<aws_secret_access_key>
```

Now, both machines have access to the Amazon S3 bucket. Perfect!


## Rich Use Cases
In this section, we will demonstrate three supported use cases, ranging from basic to advanced.

### `SlurmTask`
In the simplest use case, we specify the path to the batch script that is already available on the cluster.

Suppose we have a batch script as follows:
```
#!/bin/bash

echo "Hello AWS slurm, run a Flyte SlurmTask!" >> ./echo_aws.txt
```

We use the following python script to test Slurm agent on the [client](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#flyte-client-localhost):
```python
import os

from flytekit import workflow
from flytekitplugins.slurm import SlurmRemoteScript, SlurmTask


echo_job = SlurmTask(
    name="<task-name>",
    task_config=SlurmRemoteScript(
        slurm_host="<slurm-host>",
        batch_script_path="<remote-batch-script-path>",
        sbatch_conf={
            "partition": "debug",
            "job-name": "tiny-slurm",
        }
    )
)


@workflow
def wf() -> None:
    echo_job()


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)

    print(f">>> LOCAL EXEC <<<")
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print(result.output)
```

### `SlurmShellTask`
`SlurmShellTask` offers users the flexibility to define the content of shell scripts. Below is an example of creating a task that executes a Python script already present on the Slurm cluster:
```python
import os

from flytekit import workflow
from flytekitplugins.slurm import Slurm, SlurmShellTask


shell_task = SlurmShellTask(
    name="test-shell",
    script="""#!/bin/bash
# We can define sbatch options here, but using sbatch_conf can be more neat
echo "Run a Flyte SlurmShellTask...\n"

# Run a python script on Slurm
# Activate the virtual env first if any
python3 <path_to_python_script>
""",
    task_config=Slurm(
        slurm_host="<slurm-host>",
        sbatch_conf={
            "partition": "debug",
            "job-name": "tiny-slurm",
        }
    ),
)


@workflow
def wf() -> None:
    shell_task()


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)

    print(f">>> LOCAL EXEC <<<")
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print(result.output)
```

### `SlurmFunctionTask`
In the most advanced use case, `SlurmFunctionTask` allows users to define custom Python functions that are sent to and executed on the Slurm cluster. Following figure demonstrates the process of running a `SlurmFunctionTask`:

![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/overview_v2.png)

```python
import os

from flytekit import task, workflow
from flytekitplugins.slurm import SlurmFunction


@task(
    task_config=SlurmFunction(
        slurm_host="<slurm-host>",
        sbatch_conf={
            "partition": "debug",
            "job-name": "tiny-slurm",
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
    x = plus_one(x=x)
    msg = greet(year=x)
    return msg


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)

    print(f">>> LOCAL EXEC <<<")
    result = runner.invoke(pyflyte.main, ["run", "--raw-output-data-prefix", "<s3-bucket-uri>", path, "wf", "--x", 2024])
    print(result.output)
```

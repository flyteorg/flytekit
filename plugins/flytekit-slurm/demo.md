# Slurm Agent Demo

In this guide, we will briefly introduce how to setup an environment to test Slurm agent locally without running the backend service (e.g., flyte agent gRPC server). It covers both basic and advanced use cases.

## Table of Content
* [Overview](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#overview)
* [Setup a Local Test Environment](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#setup-a-local-test-environment)
    * [Flyte Client (Localhost)](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#flyte-client-localhost)
    * [Remote Tiny Slurm Cluster](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#remote-tiny-slurm-cluster)
    * [SSH Configuration](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#ssh-configuration)
* [Run a Demo](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/demo.md#run-a-demo)

## Overview
Slurm agent on the highest level has three core methods to interact with a Slurm cluster:
1. `create`: Use `srun` or `sbatch` to run a job on a Slurm cluster
2. `get`: Use `scontrol show job <job_id>` to monitor the Slurm job state
3. `delete`: Use `scancel <job_id>` to cancel the Slurm job (this method is still under test)

In the simplest form, Slurm agent supports directly running a batch script using `sbatch` on a Slurm cluster as shown below:

![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/basic_arch.png)

## Setup a Local Test Environment
Without running the backend service, we can setup an environment to test Slurm agent locally. The setup consists of two main components: a client (localhost) and a remote tiny Slurm cluster. Then, we need to configure SSH connection to facilitate communication between the two, which relies on `asyncssh`.

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

## Run a Demo
Suppose we have a batch script to run on Slurm cluster:
```
#!/bin/bash

echo "Working!" >> ./remote_touch.txt
```

We use the following python script to test Slurm agent on the client side. A crucial part of the task configuration is specifying the target Slurm cluster and designating the batch script's path within the cluster.

```python
import os

from flytekit import workflow
from flytekitplugins.slurm import Slurm, SlurmTask


echo_job = SlurmTask(
    name="echo-job-name",
    task_config=Slurm(
        slurm_host="<host_alias>",
        batch_script_path="<path_to_batch_script_within_cluster>",
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

After the Slurm job is completed, we can find the following result on Slurm cluster:

![](https://github.com/JiangJiaWei1103/flytekit/blob/slurm-agent-dev/plugins/flytekit-slurm/assets/slurm_basic_result.png)

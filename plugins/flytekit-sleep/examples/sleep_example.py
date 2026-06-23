"""
Sleep Plugin Example
====================

The ``core-sleep`` plugin executes entirely in the backend — no task pod is created.
The sleep duration is a normal task input, so it can be a dynamic workflow value.
"""

import os
from datetime import timedelta

from flytekitplugins.sleep import Sleep

from flytekit import task, workflow


@task(
    cache_version="2",
    task_config=Sleep()
)
def sleep_for(duration: timedelta) -> None:
    # This body only runs during local execution.
    # On the cluster, the backend sleeps for `duration` without running this.
    print(f"[local] sleeping for {duration}")


@workflow
def wf(duration: timedelta) -> None:
    sleep_for(duration=duration)


if __name__ == "__main__":
    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["--config", os.path.expanduser("~/.flyte/config-sandbox.yaml"), "run", "--remote", path, "wf", "--duration", "10s"])
    print("Remote Execution: ", result.output)

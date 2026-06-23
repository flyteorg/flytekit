"""
Sleep Fanout Example
====================

Fan out N core-sleep leaves in parallel using map_task.
No task pods are created for the leaves — the backend handles sleep directly.

Usage (remote):
    python sleep_fanout.py
"""

import os
from datetime import timedelta

from flytekitplugins.sleep import Sleep

from flytekit import task, workflow

N_CHILDREN = 20


@task(task_config=Sleep())
def sleep_leaf(duration: timedelta) -> None:
    pass


@workflow
def wf(sleep_duration: timedelta = timedelta(seconds=10)) -> None:
    for _ in range(N_CHILDREN):
        sleep_leaf(duration=sleep_duration)


if __name__ == "__main__":
    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()
    path = os.path.realpath(__file__)

    result = runner.invoke(pyflyte.main, [
        "--config", os.path.expanduser("~/.flyte/config-sandbox.yaml"),
        "run", "--remote",
        path, "wf",
        "--sleep_duration", "10s",
    ])
    print(result.output)
    if result.exception:
        raise result.exception

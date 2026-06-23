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
from typing import List

import flytekit as fl
from flytekitplugins.sleep import Sleep

from flytekit import map_task, task, workflow

sleep_image = fl.ImageSpec(
    registry="ghcr.io/machichima",
    name="sleep-fanout",
    apt_packages=["git"],
    packages=["git+https://github.com/machichima/flytekit.git@add-sleep-plugin#subdirectory=plugins/flytekit-sleep"],
    env={"REBUILD": "1"},
)


@task(container_image=sleep_image)
def make_durations(duration: timedelta, n: int) -> List[timedelta]:
    return [duration] * n


@task(task_config=Sleep(), container_image=sleep_image)
def sleep_leaf(duration: timedelta) -> None:
    pass


@workflow
def wf(sleep_duration: timedelta = timedelta(seconds=10), n_children: int = 400) -> None:
    durations = make_durations(duration=sleep_duration, n=n_children)
    map_task(sleep_leaf)(duration=durations)


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
        "--n_children", "400",
    ])
    print(result.output)
    if result.exception:
        raise result.exception

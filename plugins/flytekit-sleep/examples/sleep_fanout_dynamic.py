"""
Sleep Fanout (Dynamic) Example
===============================

Fan out N core-sleep leaves in parallel using @dynamic.
n_children is a runtime input — the @dynamic task runs in a container to
expand the subworkflow, then each leaf runs as core-sleep (no pod).

Requires flytekitplugins-sleep in the container image. The ImageSpec below
installs it from the local wheel via copy + pip install.

Usage (remote):
    python sleep_fanout_dynamic.py
"""

import os
from datetime import timedelta

import flytekit as fl
from flytekitplugins.sleep import Sleep

from flytekit import dynamic, task, workflow

fanout_image = fl.ImageSpec(
    registry="ghcr.io/machichima",
    name="sleep-fanout",
    apt_packages=["git"],
    packages=["git+https://github.com/machichima/flytekit.git@add-sleep-plugin#subdirectory=plugins/flytekit-sleep"],
)


@task(task_config=Sleep())
def sleep_leaf(duration: timedelta) -> None:
    pass


@dynamic(container_image=fanout_image)
def sleep_fanout(n_children: int, sleep_duration: timedelta) -> None:
    for _ in range(n_children):
        sleep_leaf(duration=sleep_duration)


@workflow
def wf(
    n_children: int = 10,
    sleep_duration: timedelta = timedelta(seconds=10),
) -> None:
    sleep_fanout(n_children=n_children, sleep_duration=sleep_duration)


if __name__ == "__main__":
    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()
    path = os.path.realpath(__file__)

    result = runner.invoke(pyflyte.main, [
        "--config", os.path.expanduser("~/.flyte/config-sandbox.yaml"),
        "run", "--remote",
        path, "wf",
        "--n_children", "400",
        "--sleep_duration", "10s",
    ])
    print(result.output)
    if result.exception:
        raise result.exception

import pytest
from flytekitplugins.mlflow import mlflow_autolog

from flytekit import task


@task
@mlflow_autolog()
def my_task(x: int, y: int):
    pass


def test_local_exec():
    my_task(x=1, y=1)

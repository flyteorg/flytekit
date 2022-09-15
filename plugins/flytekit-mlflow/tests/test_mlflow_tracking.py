import pytest
from flytekit import task
from flytekitplugins.mlflow import mlflow_autolog


@task
@mlflow_autolog()
def my_task(x: int, y: int):
    pass


def test_local_exec():
    my_task(x=1, y=1)

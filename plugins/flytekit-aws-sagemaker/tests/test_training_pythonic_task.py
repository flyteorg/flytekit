"""Unit tests for the Pythonic-mode SageMaker training task."""

import types

import pytest
from flytekitplugins.awssagemaker_inference.pythonic_base import (
    SAGEMAKER_PYTHONIC_BASE_IMAGE,
    PythonicSageMakerJobTask,
)
from flytekitplugins.awssagemaker_training import SageMakerTraining, SageMakerTrainingTask

from flytekit import ImageSpec, PythonFunctionTask, task
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin

ROLE = "arn:aws:iam::123456789012:role/sm-exec"
REGION = "us-east-1"


def _fn() -> int:
    return 1


def _build_task(container_image):
    return SageMakerTrainingTask(
        task_config=SageMakerTraining(execution_role_arn=ROLE, region=REGION),
        task_function=_fn,
        container_image=container_image,
    )


def test_dataclass_validation():
    with pytest.raises(ValueError):
        SageMakerTraining(execution_role_arn="", region=REGION)
    with pytest.raises(ValueError):
        SageMakerTraining(execution_role_arn=ROLE, region="")
    with pytest.raises(ValueError):
        SageMakerTraining(execution_role_arn=ROLE, region=REGION, volume_size_in_gb=0)


def test_to_from_dict_round_trip_with_output_path():
    cfg = SageMakerTraining(
        execution_role_arn=ROLE,
        region=REGION,
        instance_type="ml.m5.large",
        output_s3_path="s3://bucket/model/",
    )
    as_dict = cfg.to_dict()
    assert as_dict["output_s3_path"] == "s3://bucket/model/"
    assert SageMakerTraining.from_dict(as_dict) == cfg


def test_output_s3_path_optional_and_dropped_when_none():
    cfg = SageMakerTraining(execution_role_arn=ROLE, region=REGION)
    assert "output_s3_path" not in cfg.to_dict()


def test_default_base_image_applied_to_bare_imagespec():
    img = ImageSpec(name="smoke", registry="r")
    t = _build_task(img)
    assert t.container_image.base_image == SAGEMAKER_PYTHONIC_BASE_IMAGE


def test_get_custom_returns_config_dict():
    t = _build_task("img:tag")
    custom = t.get_custom(None)
    assert custom["execution_role_arn"] == ROLE
    assert SageMakerTraining.from_dict(custom) == t.task_config


def test_register_pythontask_plugin_wiring():
    @task(
        task_config=SageMakerTraining(execution_role_arn=ROLE, region=REGION),
        container_image="img:tag",
    )
    def my_training() -> int:
        return 1

    assert isinstance(my_training, SageMakerTrainingTask)
    assert isinstance(my_training, PythonicSageMakerJobTask)
    assert my_training.task_type == "sagemaker-training-task"


@pytest.mark.parametrize("is_local,expected", [(True, "local"), (False, "worker")])
def test_execute_dispatches_local_vs_worker(monkeypatch, is_local, expected):
    import flytekitplugins.awssagemaker_inference.pythonic_base as base

    t = _build_task("img:tag")
    monkeypatch.setattr(AsyncConnectorExecutorMixin, "execute", lambda self, **kw: "local")
    monkeypatch.setattr(PythonFunctionTask, "execute", lambda self, **kw: "worker")

    ctx = types.SimpleNamespace(
        execution_state=types.SimpleNamespace(is_local_execution=lambda: is_local)
    )
    monkeypatch.setattr(base.FlyteContextManager, "current_context", lambda: ctx)

    assert t.execute() == expected

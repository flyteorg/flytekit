"""Unit tests for the Pythonic-mode SageMaker processing task."""

import types

import pytest
from flytekitplugins.awssagemaker_inference.pythonic_base import (
    SAGEMAKER_PYTHONIC_BASE_IMAGE,
    PythonicSageMakerJobTask,
)
from flytekitplugins.awssagemaker_processing import SageMakerProcessing, SageMakerProcessingTask

from flytekit import ImageSpec, PythonFunctionTask, task
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin

ROLE = "arn:aws:iam::123456789012:role/sm-exec"
REGION = "us-east-1"


def _fn() -> int:
    return 1


def _build_task(container_image):
    return SageMakerProcessingTask(
        task_config=SageMakerProcessing(execution_role_arn=ROLE, region=REGION),
        task_function=_fn,
        container_image=container_image,
    )


def test_dataclass_validation():
    with pytest.raises(ValueError):
        SageMakerProcessing(execution_role_arn="", region=REGION)
    with pytest.raises(ValueError):
        SageMakerProcessing(execution_role_arn=ROLE, region="")
    with pytest.raises(ValueError):
        SageMakerProcessing(execution_role_arn=ROLE, region=REGION, instance_count=2)
    with pytest.raises(ValueError):
        SageMakerProcessing(execution_role_arn=ROLE, region=REGION, max_runtime_in_seconds=0)


def test_to_from_dict_round_trip():
    cfg = SageMakerProcessing(
        execution_role_arn=ROLE,
        region=REGION,
        instance_type="ml.m5.2xlarge",
        tags={"team": "ml"},
    )
    as_dict = cfg.to_dict()
    # None-valued fields are dropped.
    assert "network_config" not in as_dict
    assert as_dict["instance_count"] == 1
    assert SageMakerProcessing.from_dict(as_dict) == cfg


def test_default_base_image_applied_to_bare_imagespec():
    img = ImageSpec(name="smoke", registry="r")
    assert img.base_image is None
    t = _build_task(img)
    assert isinstance(t.container_image, ImageSpec)
    assert t.container_image.base_image == SAGEMAKER_PYTHONIC_BASE_IMAGE


def test_user_base_image_preserved():
    img = ImageSpec(name="smoke", registry="r", base_image="my/base:1")
    t = _build_task(img)
    assert t.container_image.base_image == "my/base:1"


def test_string_image_preserved():
    t = _build_task("123.dkr.ecr.us-east-1.amazonaws.com/img:tag")
    assert t.container_image == "123.dkr.ecr.us-east-1.amazonaws.com/img:tag"


def test_explicit_container_image_is_required():
    with pytest.raises(ValueError, match="explicit container_image"):
        _build_task(None)


def test_get_custom_returns_config_dict():
    t = _build_task("img:tag")
    custom = t.get_custom(None)
    assert custom["execution_role_arn"] == ROLE
    assert SageMakerProcessing.from_dict(custom) == t.task_config


def test_register_pythontask_plugin_wiring():
    @task(
        task_config=SageMakerProcessing(execution_role_arn=ROLE, region=REGION),
        container_image="img:tag",
    )
    def my_processing() -> int:
        return 1

    assert isinstance(my_processing, SageMakerProcessingTask)
    assert isinstance(my_processing, PythonicSageMakerJobTask)
    assert my_processing.task_type == "sagemaker-processing-task"


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

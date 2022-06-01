from collections import OrderedDict

import pytest
import torch

import flytekit
from flytekit import task
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
from flytekit.models.types import LiteralType
from flytekit.tools.translator import get_serializable
from flytekit.types.pytorch import PyTorchModuleTransformer, PyTorchStateDict, PyTorchTensorTransformer

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@pytest.mark.parametrize(
    "transformer,python_type,format",
    [
        (PyTorchTensorTransformer(), torch.Tensor, PyTorchTensorTransformer.PYTORCH_TENSOR_FORMAT),
        (PyTorchModuleTransformer(), PyTorchStateDict, PyTorchModuleTransformer.PYTORCH_STATEDICT_FORMAT),
    ],
)
def test_get_literal_type(transformer, python_type, format):
    tf = transformer
    lt = tf.get_literal_type(python_type)
    assert lt == LiteralType(blob=BlobType(format=format, dimensionality=BlobType.BlobDimensionality.SINGLE))


@pytest.mark.parametrize(
    "transformer,python_type,format,python_val",
    [
        (
            PyTorchTensorTransformer(),
            torch.Tensor,
            PyTorchTensorTransformer.PYTORCH_TENSOR_FORMAT,
            torch.tensor([[1, 2], [3, 4]]),
        ),
        (
            PyTorchModuleTransformer(),
            PyTorchStateDict,
            PyTorchModuleTransformer.PYTORCH_STATEDICT_FORMAT,
            PyTorchStateDict(module=torch.nn.Linear(2, 2)),
        ),
    ],
)
def test_to_python_value_and_literal(transformer, python_type, format, python_val):
    ctx = context_manager.FlyteContext.current_context()
    tf = transformer
    python_val = python_val
    lt = tf.get_literal_type(python_type)

    lv = tf.to_literal(ctx, python_val, type(python_val), lt)  # type: ignore
    assert lv.scalar.blob.metadata == BlobMetadata(
        type=BlobType(
            format=format,
            dimensionality=BlobType.BlobDimensionality.SINGLE,
        )
    )
    assert lv.scalar.blob.uri is not None

    output = tf.to_python_value(ctx, lv, python_type)
    if isinstance(python_val, torch.Tensor):
        assert torch.equal(output, python_val)
    else:
        assert isinstance(output, OrderedDict)


def test_example_tensor():
    @task
    def t1(array: torch.Tensor) -> torch.Tensor:
        return torch.flatten(array)

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert task_spec.template.interface.outputs["o0"].type.blob.format is PyTorchTensorTransformer.PYTORCH_TENSOR_FORMAT


def test_example_module():
    @task
    def t1() -> PyTorchStateDict:
        return torch.nn.BatchNorm1d(3, track_running_stats=True)

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert (
        task_spec.template.interface.outputs["o0"].type.blob.format is PyTorchModuleTransformer.PYTORCH_STATEDICT_FORMAT
    )

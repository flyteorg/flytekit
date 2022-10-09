from collections import OrderedDict

import pytest
import tensorflow

import flytekit
from flytekit import task
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.extras.tensorflow import TensorflowRecordTransformer
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
from flytekit.models.types import LiteralType
from flytekit.tools.translator import get_serializable

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
        (TensorflowRecordTransformer(), tensorflow.Tensor, TensorflowRecordTransformer.TENSORFLOW_FORMAT),
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
            TensorflowRecordTransformer(),
            tensorflow.Tensor,
            TensorflowRecordTransformer.TENSORFLOW_FORMAT,
            tensorflow.constant([[1, 2], [3, 4]]),
        )
    ],
)
def test_to_python_value_and_literal(transformer, python_type, format, python_val):
    ctx = context_manager.FlyteContext.current_context()
    tf = transformer
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
    if isinstance(python_val, tensorflow.Tensor):
        assert tensorflow.math.equal(output, python_val)
    elif isinstance(python_val, tensorflow.Module):
        for p1, p2 in zip(output.parameters(), python_val.parameters()):
            if p1.data.ne(p2.data).sum() > 0:
                assert False
        assert True
    else:
        assert isinstance(output, dict)


def test_example_tensor():
    @task
    def t1(array: tensorflow.Tensor) -> tensorflow.Tensor:
        return tensorflow.reshape(array, [-1])

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert task_spec.template.interface.outputs["o0"].type.blob.format is TensorflowRecordTransformer.TENSORFLOW_FORMAT

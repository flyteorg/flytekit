from collections import OrderedDict

import keras
import numpy as np
import pytest

import flytekit
from flytekit import task
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.extras.keras import KerasSequentialTransformer
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


def build_keras_sequential_model():
    model = keras.Sequential()
    model.add(keras.Input(shape=(16,)))
    model.add(keras.layers.Dense(8))
    model.add(keras.layers.Dense(1))
    model.compile(optimizer="sgd", loss="mse")
    return model


@pytest.mark.parametrize(
    "transformer,python_type,format",
    [
        (KerasSequentialTransformer(), keras.Sequential, KerasSequentialTransformer.KERAS_FORMAT),
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
            KerasSequentialTransformer(),
            keras.Sequential,
            KerasSequentialTransformer.KERAS_FORMAT,
            build_keras_sequential_model(),
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
    if isinstance(python_val, keras.Sequential):
        for p1, p2 in zip(output.weights, python_val.weights):
            np.testing.assert_array_equal(p1.numpy(), p2.numpy())
        assert True
    else:
        assert isinstance(output, dict)


def test_example_model():
    @task
    def t1() -> keras.Sequential:
        return build_keras_sequential_model()

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert task_spec.template.interface.outputs["o0"].type.blob.format is KerasSequentialTransformer.KERAS_FORMAT

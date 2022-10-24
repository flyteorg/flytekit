import pytest
import tensorflow as tf

import flytekit
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.extras.tensorflow.record import TensorflowExampleTransformer
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
from flytekit.models.types import LiteralType

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)

a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))

features = tf.train.Features(
    feature=dict(
        a=tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"])),
        b=tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0])),
    )
)


@pytest.mark.parametrize(
    "transformer,python_type,format",
    [
        (TensorflowExampleTransformer(), tf.train.Example, TensorflowExampleTransformer.TENSORFLOW_FORMAT),
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
            TensorflowExampleTransformer(),
            tf.train.Example,
            TensorflowExampleTransformer.TENSORFLOW_FORMAT,
            tf.train.Example(features=features),
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
    assert output.features.feature == python_val.features.feature

from typing import Type

import pytest
import tensorflow as tf

import flytekit
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.extras.tensorflow.record import TensorflowExampleRecordsTransformer, TFRecordDatasetConfig
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType
from flytekit.types.directory import TFRecordsDirectory
from flytekit.types.file import TFRecordFile

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def write_to_tfrecord(path):
    a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
    b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))
    c = tf.train.Feature(int64_list=tf.train.Int64List(value=[3, 4]))
    features = tf.train.Features(feature=dict(a=a, b=b, c=c))
    example = tf.train.Example(features=features).SerializeToString()
    with tf.io.TFRecordWriter(path) as writer:
        for i in range(2):
            writer.write(example)


@pytest.mark.parametrize(
    "transformer,python_type,format",
    [
        (TensorflowExampleRecordsTransformer(), TFRecordFile, TensorflowExampleRecordsTransformer.TENSORFLOW_FORMAT),
        (
            TensorflowExampleRecordsTransformer(),
            TFRecordsDirectory,
            TensorflowExampleRecordsTransformer.TENSORFLOW_FORMAT,
        ),
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
            TensorflowExampleRecordsTransformer(),
            Type[TFRecordFile],
            TensorflowExampleRecordsTransformer.TENSORFLOW_FORMAT,
            TFRecordFile,
        ),
        (
            TensorflowExampleRecordsTransformer(),
            Type[TFRecordsDirectory],
            TensorflowExampleRecordsTransformer.TENSORFLOW_FORMAT,
            TFRecordsDirectory,
        ),
    ],
)
def test_to_literal(transformer, python_type, format, python_val):
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


@pytest.mark.parametrize(
    "transformer,python_type,format,python_val",
    [
        (
            TensorflowExampleRecordsTransformer(),
            TFRecordDatasetConfig(name="example_test"),
            TensorflowExampleRecordsTransformer.TENSORFLOW_FORMAT,
            tf.data.TFRecordDataset,
        )
    ],
)
def test_to_python_value(transformer, python_type, format, python_val):
    ctx = context_manager.FlyteContext.current_context()
    tf = transformer
    meta = BlobMetadata(
        type=BlobType(
            format=format,
            dimensionality=BlobType.BlobDimensionality.SINGLE,
        )
    )
    local_path = ctx.file_access.get_random_local_directory() + "000.tfrecord"
    remote_path = ctx.file_access.get_random_remote_path(local_path)
    write_to_tfrecord(local_path)
    lv = Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))
    output = tf.to_python_value(ctx, lv, python_type)
    assert output.features.feature == python_val.features.feature

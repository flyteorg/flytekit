from typing import Annotated

import pytest
import tensorflow
import tensorflow as tf

import flytekit
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.extras.tensorflow.record import (
    TensorFlowRecordFileTransformer,
    TensorFlowRecordsDirTransformer,
    TFRecordDatasetConfig,
    TFRecordDatasetV2,
)
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
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

a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))
c = tf.train.Feature(int64_list=tf.train.Int64List(value=[3, 4]))
features = tf.train.Features(feature=dict(a=a, b=b, c=c))


@pytest.mark.parametrize(
    "transformer,python_type,format,dimensionality",
    [
        (TensorFlowRecordFileTransformer(), TFRecordFile, TensorFlowRecordFileTransformer.TENSORFLOW_FORMAT, 0),
        (TensorFlowRecordsDirTransformer(), TFRecordsDirectory, TensorFlowRecordsDirTransformer.TENSORFLOW_FORMAT, 1),
    ],
)
def test_get_literal_type(transformer, python_type, format, dimensionality):
    tf = transformer
    lt = tf.get_literal_type(python_type)
    assert lt == LiteralType(blob=BlobType(format=format, dimensionality=dimensionality))


@pytest.mark.parametrize(
    "transformer,python_type,format,python_val,dimension",
    [
        (
            TensorFlowRecordFileTransformer(),
            TFRecordFile,
            TensorFlowRecordFileTransformer.TENSORFLOW_FORMAT,
            tf.train.Example(features=features),
            BlobType.BlobDimensionality.SINGLE,
        ),
        (
            TensorFlowRecordsDirTransformer(),
            TFRecordsDirectory,
            TensorFlowRecordsDirTransformer.TENSORFLOW_FORMAT,
            [tf.train.Example(features=features)] * 2,
            BlobType.BlobDimensionality.MULTIPART,
        ),
    ],
)
def test_to_python_value_and_literal(transformer, python_type, format, python_val, dimension):
    ctx = context_manager.FlyteContext.current_context()
    tf = transformer
    lt = tf.get_literal_type(python_type)
    lv = tf.to_literal(ctx, python_val, type(python_val), lt)  # type: ignore
    assert lv.scalar.blob.metadata == BlobMetadata(
        type=BlobType(
            format=format,
            dimensionality=dimension,
        )
    )
    assert lv.scalar.blob.uri is not None
    output = tf.to_python_value(ctx, lv, Annotated[TFRecordDatasetV2, TFRecordDatasetConfig(name="example_test")])
    assert isinstance(output, TFRecordDatasetV2)
    example = tensorflow.train.Example()
    for raw_record in output:
        example.ParseFromString(raw_record.numpy())
    if isinstance(python_val, list):
        assert example == tensorflow.train.Example(features=tensorflow.train.Features(feature=dict(a=a, b=b, c=c)))
    else:
        assert example == python_val

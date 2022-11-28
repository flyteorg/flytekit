from typing import Annotated, Dict, Tuple

import numpy as np
import tensorflow as tf

from flytekit import task, workflow
from flytekit.extras.tensorflow.record import TFRecordDatasetConfig
from flytekit.types.directory import TFRecordsDirectory
from flytekit.types.file import TFRecordFile

a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))
c = tf.train.Feature(int64_list=tf.train.Int64List(value=[3, 4]))


@task
def generate_tf_record_file() -> TFRecordFile:
    features = tf.train.Features(feature=dict(a=a, b=b, c=c))
    return tf.train.Example(features=features)


@task
def generate_tf_record_dir() -> TFRecordsDirectory:
    features = tf.train.Features(feature=dict(a=a, b=b, c=c))
    return [tf.train.Example(features=features)] * 2


@task
def t1(
    dataset: Annotated[
        TFRecordFile,
        TFRecordDatasetConfig(name="testing", buffer_size=1024, num_parallel_reads=3, compression_type="GZIP"),
    ]
):
    assert dataset._filenames._name == "testing"
    assert dataset._compression_type == "GZIP"
    assert dataset._buffer_size == 1024
    assert dataset._num_parallel_reads == 3


@task
def t2(dataset: Annotated[TFRecordFile, TFRecordDatasetConfig(name="production", buffer_size=512)]):
    assert dataset._filenames._name == "production"
    assert dataset._compression_type is None
    assert dataset._buffer_size == 512
    assert dataset._num_parallel_reads is None


@task
def t3(dataset: Annotated[TFRecordFile, TFRecordDatasetConfig(name="testing")]) -> Dict[str, np.ndarray]:
    example = tf.train.Example()
    for batch in list(dataset.as_numpy_iterator()):
        example.ParseFromString(batch)

    result = {}
    for key, feature in example.features.feature.items():
        kind = feature.WhichOneof("kind")
        result[key] = np.array(getattr(feature, kind).value)

    return result


@workflow
def wf() -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
    file = generate_tf_record_file()
    files = generate_tf_record_dir()
    t1(dataset=file)
    t2(dataset=file)
    return t3(dataset=file), t3(dataset=files)


def test_wf():
    file_res, dir_res = wf()
    assert np.array_equal(file_res["a"], np.array([b"foo", b"bar"]))
    assert np.array_equal(file_res["b"], np.array([1.0, 2.0]))
    assert np.array_equal(file_res["c"], np.array([3, 4]))

    assert np.array_equal(dir_res["a"], np.array([b"foo", b"bar"]))
    assert np.array_equal(dir_res["b"], np.array([1.0, 2.0]))
    assert np.array_equal(dir_res["c"], np.array([3, 4]))

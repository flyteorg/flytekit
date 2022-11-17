from typing import Annotated

import tensorflow as tf

from flytekit import task, workflow
from flytekit.extras.tensorflow.record import TFRecordDatasetConfig
from flytekit.types.directory import TFRecordsDirectory
from flytekit.types.file import TFRecordFile

a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))
c = tf.train.Feature(int64_list=tf.train.Int64List(value=[3, 4]))


def decode_fn(record_bytes):
    return tf.io.parse_single_example(
        record_bytes,
        {
            "a": tf.io.FixedLenFeature([], dtype=tf.string),
            "b": tf.io.FixedLenFeature([], dtype=tf.float32),
            "c": tf.io.FixedLenFeature([], dtype=tf.int64),
        },
    )


@task
def generate_tf_record_file() -> TFRecordFile:
    features = tf.train.Features(feature=dict(a=a, b=b, c=c))
    return tf.train.Example(features=features)


@task
def generate_tf_record_dir() -> TFRecordsDirectory:
    features = tf.train.Features(feature=dict(a=a, b=b, c=c))
    return [tf.train.Example(features=features)] * 2


@task
def consume(dataset: Annotated[tf.data.TFRecordDataset, TFRecordDatasetConfig(name="testing")]):
    for batch in dataset.map(decode_fn):
        print("x = {x:.4f},  y = {y:.4f}".format(**batch))


@workflow
def wf():
    file = generate_tf_record_file()
    files = generate_tf_record_dir()
    consume(dataset=file)
    consume(dataset=files)


@workflow
def test_wf():
    wf()

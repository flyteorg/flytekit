import tensorflow as tf

from flytekit import task, workflow


@task
def generate_tf_example() -> tf.train.Example:
    a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
    b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))
    features = tf.train.Features(feature=dict(a=a, b=b))
    return tf.train.Example(features=features)


@task
def t1(example: tf.train.Example) -> tf.train.Example:
    assert example.features.feature["a"].bytes_list.value == [b"foo", b"bar"]
    assert example.features.feature["b"].float_list.value == [1.0, 2.0]
    return example


@task
def t2(example: tf.train.Example) -> tf.train.Example:
    # add a third feature
    int_feat = tf.train.Feature(int64_list=tf.train.Int64List(value=[3, 4]))
    example.features.feature.get_or_create("c")
    example.features.feature.setdefault("c", int_feat)
    return example


@workflow
def wf() -> tf.train.Example:
    t1(tensor=generate_tf_example())
    result = t2(tensor=generate_tf_example())
    return result


@workflow
def test_wf():
    example = wf()
    assert example.features.feature["c"].int64_list.value == [3, 4]

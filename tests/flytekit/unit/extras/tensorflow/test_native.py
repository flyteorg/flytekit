import tensorflow as tf

from flytekit import task, workflow

a = tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"foo", b"bar"]))
b = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0, 2.0]))
c = tf.train.Feature(int64_list=tf.train.Int64List(value=[3, 4]))


@task
def generate_tf_example_1() -> tf.train.Example:
    features = tf.train.Features(feature=dict(a=a, b=b))
    return tf.train.Example(features=features)


@task
def generate_tf_example_2() -> tf.train.Example:
    features = tf.train.Features(feature=dict(a=a, b=b, c=c))
    return tf.train.Example(features=features)


@task
def t1(example: tf.train.Example) -> tf.train.Example:
    assert example.features.feature["a"].bytes_list.value == [b"foo", b"bar"]
    assert example.features.feature["b"].float_list.value == [1.0, 2.0]
    return example


@task
def t2(example: tf.train.Example) -> tf.train.Example:
    assert example.features.feature["c"].int64_list.value == [3, 4]
    return example


@task
def t3(example: tf.train.Example):
    feature_description = {
        "b": tf.io.RaggedFeature(dtype=float),
        "c": tf.io.RaggedFeature(dtype=tf.int64),
    }
    parsed = tf.io.parse_example(example.SerializeToString(), feature_description)
    result = dict(map(lambda x: (x[0], x[1].numpy().tolist()), parsed.items()))
    assert result == {"b": [1.0, 2.0], "c": [3, 4]}


@workflow
def wf():
    t1(example=generate_tf_example_1())
    input_t3 = t2(example=generate_tf_example_2())
    t3(example=input_t3)


@workflow
def test_wf():
    wf()

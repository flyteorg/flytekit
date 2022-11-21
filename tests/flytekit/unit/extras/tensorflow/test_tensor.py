import tensorflow as tf

from flytekit import task, workflow
from flytekit.extras import tensorflow


@task
def create_rank1_tensor() -> tf.Tensor:
    return tf.constant([6, 5, 4, 3, 2, 1], dtype=tf.int32)


@task
def create_rank2_tensor() -> tf.Tensor:
    return tf.constant([[1, 2, 3], [4, 5, 6], [7, 8, 9]])


@task
def create_rank3_tensor() -> tf.Tensor:
    return tf.constant(
        [
            [[0.0, 1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0, 9.0]],
            [[10.0, 11.0, 12.0, 13.0, 14.0], [15.0, 16.0, 17.0, 18.0, 19.0]],
            [[20.0, 21.0, 22.0, 21.0, 24.0], [25.0, 26.0, 27.0, 28.0, 29.0]],
        ],
        dtype=tf.float64,
    )


@task
def task1(tensor: tf.Tensor) -> tf.Tensor:
    assert tensor.dtype == tf.int32
    return tensor


@task
def task2(tensor: tf.Tensor) -> tf.Tensor:
    return tf.reshape(tensor, [1, 9])


@task
def task3(tensor: tf.Tensor) -> tf.Tensor:
    assert tensor.dtype == tf.float64
    return tensor


def wf():
    task1(tensor=create_rank1_tensor())
    task2(tensor=create_rank2_tensor())
    task3(tensor=create_rank3_tensor())


@workflow
def test_wf():
    wf()

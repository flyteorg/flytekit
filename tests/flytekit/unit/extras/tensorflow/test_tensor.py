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
            [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]],
            [[10, 11, 12, 13, 14], [15, 16, 17, 18, 19]],
            [[20, 21, 22, 21, 24], [25, 26, 27, 28, 29]],
        ]
    )


@task
def task1(tensor: tf.Tensor) -> tf.Tensor:
    assert tensor.dtype == tf.int32
    return tensor


@task
def task2(tensor: tf.Tensor) -> tf.Tensor:
    return tf.reshape(tensor, [2, 3])


@workflow
def wf():
    task1(tensor=create_rank1_tensor())
    task2(tensor=create_rank1_tensor())


@workflow
def test_wf():
    wf()

from typing import Any, List

import tensorflow as tf

from flytekit import task, workflow


@task
def get_layer() -> tf.keras.layers.Dense:
    layer = tf.keras.layers.Dense(10)
    layer(tf.ones((10, 1)))
    return layer


@task
def generate_sequential_model() -> List[tf.keras.layers.Layer]:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.Input(shape=(32,)),
            tf.keras.layers.Dense(1),
        ]
    )
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
        loss=tf.keras.losses.BinaryCrossentropy(),
        metrics=[
            tf.keras.metrics.BinaryAccuracy(),
        ],
    )
    return model.layers


@task
def get_layers_weights(layers: List[tf.keras.layers.Layer]) -> List[Any]:
    return layers[-1].weights


@workflow
def wf():
    dense_layer = get_layer()
    layers = generate_sequential_model()
    get_layers_weights(layers=[dense_layer])
    get_layers_weights(layers=layers)


@workflow
def test_wf():
    wf()

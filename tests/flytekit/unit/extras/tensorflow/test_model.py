from typing import List

import tensorflow as tf

from flytekit import task, workflow


@task
def generate_model() -> tf.keras.Model:
    inputs = tf.keras.Input(shape=(32,))
    outputs = tf.keras.layers.Dense(1)(inputs)
    model = tf.keras.Model(inputs, outputs)
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
        loss=tf.keras.losses.BinaryCrossentropy(),
        metrics=[
            tf.keras.metrics.BinaryAccuracy(),
        ],
    )
    return model


@task
def generate_sequential_model() -> tf.keras.Model:
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
    return model


@task
def get_model_layers(model: tf.keras.Model) -> List[tf.keras.layers.Layer]:
    return model.layers


@workflow
def wf():
    model1 = generate_model()
    model2 = generate_sequential_model()
    get_model_layers(model=model1)
    get_model_layers(model=model2)


@workflow
def test_wf():
    wf()

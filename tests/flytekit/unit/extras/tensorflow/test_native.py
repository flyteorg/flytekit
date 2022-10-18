import tensorflow as tf
from typing import List

from flytekit import task, workflow


@task
def generate_model() -> tf.keras.Model:
    inputs = tf.keras.Input(shape=(32,))
    outputs = tf.keras.layers.Dense(1)(inputs)
    model = tf.keras.Model(inputs, outputs)
    return model


@task
def get_model_layers(model: tf.keras.Model) -> List[tf.keras.layers.Layer]:
    return model.layers


@workflow
def wf():
    model = generate_model()
    get_model_layers(model=model)


@workflow
def test_wf():
    wf()

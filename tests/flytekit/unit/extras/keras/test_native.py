from typing import List

import keras
import numpy as np

from flytekit import task, workflow


@task
def get_model_with_sequential_class() -> keras.Sequential:
    model = keras.Sequential()
    model.add(keras.layers.Dense(8, input_shape=(16,)))
    model.add(keras.layers.Dense(4))
    return model


@task
def get_model_with_model_class() -> keras.Model:
    inputs = keras.Input(shape=(3,))
    x = keras.layers.Dense(4)(inputs)
    outputs = keras.layers.Dense(5)(x)
    model = keras.Model(inputs=inputs, outputs=outputs)
    return model


@task
def get_model_weights(model: keras.Sequential) -> List[np.array]:
    assert len(model.weights) == 4
    return model.weights


@task
def get_model_layer(model: keras.Sequential) -> List[keras.layers.core.dense.Dense]:
    if isinstance(model, keras.Sequential):
        assert len(model.layers) == 2
    elif isinstance(model, keras.Model):
        assert len(model.layers) == 3
    return model.layers


@workflow
def wf():
    models = (get_model_with_sequential_class(), get_model_with_model_class())
    for m in models:
        get_model_weights(model=m)
        get_model_layer(model=m)


@workflow
def test_wf():
    wf()

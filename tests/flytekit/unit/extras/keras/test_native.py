from typing import List

import keras
import numpy as np

from flytekit import task, workflow


@task
def generate_model() -> keras.Sequential:
    model = keras.Sequential()
    model.add(keras.layers.Dense(8, input_shape=(16,)))
    model.add(keras.layers.Dense(4))
    return model


@task
def get_model_weights(model: keras.Sequential) -> List[np.array]:
    assert len(model.weights) == 4
    return model.weights


@workflow
def wf():
    model = generate_model()
    get_model_weights(model=model)


@workflow
def test_wf():
    wf()

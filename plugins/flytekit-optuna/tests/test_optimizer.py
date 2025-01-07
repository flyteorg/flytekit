import asyncio
import math
from typing import Union

import flytekit as fl
from flytekitplugins.optuna import Optimizer, suggest


image = fl.ImageSpec(builder="union", packages=["flytekit>=1.15.0", "optuna>=4.0.0"])

@fl.task(container_image=image)
async def objective(x: float, y: int, z: int, power: int) -> float:
    return math.log((((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2)) ** power


@fl.eager(container_image=image)
async def train(concurrency: int, n_trials: int) -> float:
    optimizer = Optimizer(objective, concurrency, n_trials)

    await optimizer(
        x=suggest.float(low=-10, high=10),
        y=suggest.integer(low=-10, high=10),
        z=suggest.category([-5, 0, 3, 6, 9]),
        power=2,
    )

    return optimizer.study.best_value

def test_local_exec():

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)


@fl.task(container_image=image)
async def bundled_objective(suggestions: dict[str, Union[int, float]], power: int) -> float:

    # building out a large set of typed inputs is exhausting, so we can just use a dict

    x, y, z = suggestions["x"], suggestions["y"], suggestions["z"]

    return math.log((((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2)) ** power


@fl.eager(container_image=image)
async def train(concurrency: int, n_trials: int) -> float:
    optimizer = Optimizer(objective, concurrency, n_trials)

    suggestions = {
        "x": suggest.float(low=-10, high=10),
        "y": suggest.integer(low=-10, high=10),
        "z": suggest.category([-5, 0, 3, 6, 9]),
    }

    await optimizer(suggestions, power=2)

    return optimizer.study.best_value

def test_bundled_local_exec():

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

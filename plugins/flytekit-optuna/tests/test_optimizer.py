import asyncio
import math
from typing import Union

import flytekit as fl
from flytekitplugins.optuna import Optimizer, suggest





def test_local_exec():


    @fl.task
    async def objective(x: float, y: int, z: int, power: int) -> float:
        return math.log((((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2)) ** power


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:
        optimizer = Optimizer(objective, concurrency, n_trials)

        await optimizer(
            x=suggest.float(low=-10, high=10),
            y=suggest.integer(low=-10, high=10),
            z=suggest.category([-5, 0, 3, 6, 9]),
            power=2,
        )

        return optimizer.study.best_value

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)



def test_bundled_local_exec():

    @fl.task
    async def objective(suggestions: dict[str, Union[int, float]], z: int, power: int) -> float:

        # building out a large set of typed inputs is exhausting, so we can just use a dict

        x, y = suggestions["x"], suggestions["y"]

        return math.log((((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2)) ** power


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:
        optimizer = Optimizer(objective, concurrency, n_trials)

        suggestions = {
            "x": suggest.float(low=-10, high=10),
            "y": suggest.integer(low=-10, high=10),
        }

        await optimizer(suggestions, z=suggest.category([-5, 0, 3, 6, 9]), power=2)

        return optimizer.study.best_value
    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

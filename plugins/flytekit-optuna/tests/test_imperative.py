from typing import Union

import asyncio
from typing import Union

import optuna
import flytekit as fl
from flytekitplugins.optuna import Optimizer, suggest



def test_local_exec():


    @fl.task
    async def objective(x: float, y: int, z: int, power: int) -> float:
        return (((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2) ** power


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:
        optimizer = Optimizer(objective, concurrency=concurrency, n_trials=n_trials)

        await optimizer(
            x=suggest.float(low=-10, high=10),
            y=suggest.integer(low=-10, high=10),
            z=suggest.category([-5, 0, 3, 6, 9]),
            power=2,
        )

        return optimizer.study.best_value

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

def test_tuple_out():

    @fl.task
    async def objective(x: float, y: int, z: int, power: int) -> tuple[float, float]:

        y0 = (((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2) ** power
        y1 = (((x - 2) ** 4) + (y + 1) ** 2 + (4 * z - 1))

        return y0, y1


    @fl.eager
    async def train(concurrency: int, n_trials: int):
        optimizer = Optimizer(
            objective=objective,
            concurrency=concurrency,
            n_trials=n_trials,
            study=optuna.create_study(directions=["maximize", "maximize"])
        )

        await optimizer(
            x=suggest.float(low=-10, high=10),
            y=suggest.integer(low=-10, high=10),
            z=suggest.category([-5, 0, 3, 6, 9]),
            power=2,
        )

    asyncio.run(train(concurrency=2, n_trials=10))


def test_bundled_local_exec():

    @fl.task
    async def objective(suggestions: dict[str, Union[int, float]], z: int, power: int) -> float:

        # building out a large set of typed inputs is exhausting, so we can just use a dict

        x, y = suggestions["x"], suggestions["y"]

        return (((x - 5) ** 2) + (y + 4) ** 4) ** power


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:
        optimizer = Optimizer(objective, concurrency=concurrency, n_trials=n_trials)

        suggestions = {
            "x": suggest.float(low=-10, high=10),
            "y": suggest.integer(low=-10, high=10),
        }

        await optimizer(
            suggestions=suggestions,
            z=suggest.category([-5, 0, 3, 6, 9]),
            power=2,
        )

        return optimizer.study.best_value
    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

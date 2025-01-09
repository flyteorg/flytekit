from typing import Any, Union

import asyncio
import math
from typing import Union
import optuna

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

        await optimizer(
            suggestions=suggestions,
            z=suggest.category([-5, 0, 3, 6, 9]),
            power=2,
        )

        return optimizer.study.best_value
    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)


def test_callback():


    @fl.task
    async def objective(letter: str, number: Union[float, int], other: str, fixed: str) -> float:

        loss = len(letter) + number + len(other) + len(fixed)

        return float(loss)


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:

        study = optuna.create_study(direction="maximize")

        def callback(trial: optuna.Trial, inputs: dict[str, Any]) -> dict[str, Any]:

            letter = trial.suggest_categorical("booster", ["A", "B", "BLAH"])

            if letter == "A":
                number = trial.suggest_int("number_A", 1, 10)
            elif letter == "B":
                number = trial.suggest_float("number_B", 10., 20.)
            else:
                number = 10

            other = trial.suggest_categorical("other", ["Something", "another word", "a phrase"])

            return dict(other=other, number=number, letter=letter, fixed=inputs["fixed"])

        optimizer = Optimizer(objective, concurrency, n_trials, study=study, callback=callback)

        await optimizer(fixed="hello!")
        
        return float(optimizer.study.best_value)

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

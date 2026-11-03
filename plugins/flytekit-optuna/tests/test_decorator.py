from typing import Union
import math

import asyncio
from typing import Union
import optuna

import flytekit as fl
from flytekitplugins.optuna import optimize, suggest


def test_local_exec():

    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:

        @optimize(concurrency=concurrency, n_trials=n_trials)
        @fl.task
        async def optimizer(x: float, y: int, z: int, power: int) -> float:
            return (((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2) ** power

        await optimizer(
            x=suggest.float(low=-10, high=10),
            y=suggest.integer(low=-10, high=10),
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

    @optimize(n_trials=10, concurrency=2)
    def optimizer(trial: optuna.Trial, fixed: str):

        letter = trial.suggest_categorical("booster", ["A", "B", "BLAH"])

        if letter == "A":
            number = trial.suggest_int("number_A", 1, 10)
        elif letter == "B":
            number = trial.suggest_float("number_B", 10., 20.)
        else:
            number = 10

        other = trial.suggest_categorical("other", ["Something", "another word", "a phrase"])

        return objective(letter, number, other, fixed)


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:

        await optimizer(fixed="hello!")

        return float(optimizer.study.best_value)

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)


def test_unparameterized_callback():


    @fl.task
    async def objective(letter: str, number: Union[float, int], other: str, fixed: str) -> float:

        loss = len(letter) + number + len(other) + len(fixed)

        return float(loss)

    @optimize
    def optimizer(trial: optuna.Trial, fixed: str):

        letter = trial.suggest_categorical("booster", ["A", "B", "BLAH"])

        if letter == "A":
            number = trial.suggest_int("number_A", 1, 10)
        elif letter == "B":
            number = trial.suggest_float("number_B", 10., 20.)
        else:
            number = 10

        other = trial.suggest_categorical("other", ["Something", "another word", "a phrase"])

        return objective(letter, number, other, fixed)


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:

        optimizer.n_trials = n_trials
        optimizer.concurrency = concurrency

        await optimizer(fixed="hello!")

        return float(optimizer.study.best_value)

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

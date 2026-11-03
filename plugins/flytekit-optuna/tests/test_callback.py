from typing import Union

import asyncio
from typing import Union
import optuna

import flytekit as fl
from flytekitplugins.optuna import Optimizer


def test_callback():


    @fl.task
    async def objective(letter: str, number: Union[float, int], other: str, fixed: str) -> float:

        loss = len(letter) + number + len(other) + len(fixed)

        return float(loss)

    def callback(trial: optuna.Trial, fixed: str):

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

        study = optuna.create_study(direction="maximize")

        optimizer = Optimizer(callback, concurrency=concurrency, n_trials=n_trials, study=study)

        await optimizer(fixed="hello!")

        return float(optimizer.study.best_value)

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)


def test_async_callback():

    @fl.task
    async def objective(letter: str, number: Union[float, int], other: str, fixed: str) -> float:

        loss = len(letter) + number + len(other) + len(fixed)

        return float(loss)

    async def callback(trial: optuna.Trial, fixed: str):

        letter = trial.suggest_categorical("booster", ["A", "B", "BLAH"])

        if letter == "A":
            number = trial.suggest_int("number_A", 1, 10)
        elif letter == "B":
            number = trial.suggest_float("number_B", 10., 20.)
        else:
            number = 10

        other = trial.suggest_categorical("other", ["Something", "another word", "a phrase"])

        return await objective(letter, number, other, fixed)


    @fl.eager
    async def train(concurrency: int, n_trials: int) -> float:

        study = optuna.create_study(direction="maximize")

        optimizer = Optimizer(callback, concurrency=concurrency, n_trials=n_trials, study=study)

        await optimizer(fixed="hello!")

        return float(optimizer.study.best_value)

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

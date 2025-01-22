import asyncio
import inspect
from copy import copy, deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Optional, Union

from typing_extensions import Concatenate, ParamSpec

import optuna
from flytekit.core.python_function_task import AsyncPythonFunctionTask
from flytekit.exceptions.eager import EagerException


class Suggestion: ...


class Number(Suggestion):
    def __post_init__(self):
        if self.low >= self.high:
            raise ValueError("low must be less than high")

        if self.step is not None and self.step > (self.high - self.low):
            raise ValueError("step must be less than the range of the suggestion")


@dataclass
class Float(Number):
    low: float
    high: float
    step: Optional[float] = None
    log: bool = False


@dataclass
class Integer(Number):
    low: int
    high: int
    step: int = 1
    log: bool = False


@dataclass
class Category(Suggestion):
    choices: list[Any]


suggest = SimpleNamespace(float=Float, integer=Integer, category=Category)

P = ParamSpec("P")

Result = Union[float, tuple[float, ...]]

CallbackType = Callable[Concatenate[optuna.Trial, P], Union[Awaitable[Result], Result]]


@dataclass
class Optimizer:
    objective: Union[CallbackType, AsyncPythonFunctionTask]
    concurrency: int
    n_trials: int
    study: Optional[optuna.Study] = None
    delay: int = 0

    """
    Optimizer is a class that allows for the distributed optimization of a flytekit Task using Optuna.

    Args:
        objective: The objective function to be optimized. This can be a AsyncPythonFunctionTask or a callable.
        concurrency: The number of trials to run concurrently.
        n_trials: The number of trials to run in total.
        study: The study to use for optimization. If None, a new study will be created.
        delay: The delay in seconds between starting each trial. Default is 0.
    """

    @property
    def is_imperative(self) -> bool:
        return isinstance(self.objective, AsyncPythonFunctionTask)

    def __post_init__(self):
        if self.study is None:
            self.study = optuna.create_study()

        if (not isinstance(self.concurrency, int)) or (not self.concurrency > 0):
            raise ValueError("concurrency must be an integer greater than 0")

        if (not isinstance(self.n_trials, int)) or (not self.n_trials > 0):
            raise ValueError("n_trials must be an integer greater than 0")

        if not isinstance(self.study, optuna.Study):
            raise ValueError("study must be an optuna.Study")

        if not isinstance(self.delay, int) or (not self.delay >= 0):
            raise ValueError("delay must be an integer greater than or equal to 0")

        if self.is_imperative:
            signature = inspect.signature(self.objective.task_function)

            if signature.return_annotation is float:
                if len(self.study.directions) != 1:
                    raise ValueError("the study must have a single objective if objective returns a single float")

            elif hasattr(signature.return_annotation, "__args__"):
                args = signature.return_annotation.__args__
                if len(args) != len(self.study.directions):
                    raise ValueError("objective must return the same number of directions in the study")

                if not all(arg is float for arg in args):
                    raise ValueError("objective function must return a float or tuple of floats")

            else:
                raise ValueError("objective function must return a float or tuple of floats")

        else:
            if not callable(self.objective):
                raise ValueError("objective must be a callable or a AsyncPythonFunctionTask")

            signature = inspect.signature(self.objective)

            if "trial" not in signature.parameters:
                raise ValueError(
                    "objective function must have a parameter called 'trial' if not a AsyncPythonFunctionTask"
                )

    async def __call__(self, **inputs: P.kwargs):
        """
        Asynchronously executes the objective function remotely.
        Parameters:
            **inputs: inputs to objective function
        """

        # create semaphore to manage concurrency
        semaphore = asyncio.Semaphore(self.concurrency)

        # create list of async trials
        trials = [self.spawn(semaphore, deepcopy(inputs)) for _ in range(self.n_trials)]

        # await all trials to complete
        await asyncio.gather(*trials)

    async def spawn(self, semaphore: asyncio.Semaphore, inputs: dict[str, Any]):
        async with semaphore:
            await asyncio.sleep(self.delay)

            # ask for a new trial
            trial: optuna.Trial = self.study.ask()

            try:
                result: Union[float, tuple[float, ...]]

                # schedule the trial
                if self.is_imperative:
                    result = await self.objective(**process(trial, inputs))

                else:
                    out = self.objective(trial=trial, **inputs)
                    result = out if not inspect.isawaitable(out) else await out

                # tell the study the result
                self.study.tell(trial, result, state=optuna.trial.TrialState.COMPLETE)

            # if the trial fails, tell the study
            except EagerException:
                self.study.tell(trial, state=optuna.trial.TrialState.FAIL)


def optimize(
    objective: Optional[Union[CallbackType, AsyncPythonFunctionTask]] = None,
    concurrency: int = 1,
    n_trials: int = 1,
    study: Optional[optuna.Study] = None,
):
    if objective is not None:
        if callable(objective) or isinstance(objective, AsyncPythonFunctionTask):
            return Optimizer(
                objective=objective,
                concurrency=concurrency,
                n_trials=n_trials,
                study=study,
            )

        else:
            raise ValueError("This decorator must be called with a callable or a flyte Task")
    else:

        def decorator(objective):
            return Optimizer(objective=objective, concurrency=concurrency, n_trials=n_trials, study=study)

        return decorator


def process(trial: optuna.Trial, inputs: dict[str, Any], root: Optional[list[str]] = None) -> dict[str, Any]:
    if root is None:
        root = []

    suggesters = {
        Float: trial.suggest_float,
        Integer: trial.suggest_int,
        Category: trial.suggest_categorical,
    }

    for key, value in inputs.items():
        path = copy(root) + [key]

        if isinstance(inputs[key], Suggestion):
            suggester = suggesters[type(value)]
            inputs[key] = suggester(name=(".").join(path), **vars(value))

        elif isinstance(value, dict):
            inputs[key] = process(trial=trial, inputs=value, root=path)

    return inputs

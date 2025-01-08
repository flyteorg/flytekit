import asyncio
import inspect
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional, Union

import optuna
from flytekit import PythonFunctionTask
from flytekit.core.workflow import PythonFunctionWorkflow
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


@dataclass
class Optimizer:
    objective: Union[PythonFunctionTask, PythonFunctionWorkflow]
    concurrency: int
    n_trials: int
    study: Optional[optuna.Study] = None

    def __post_init__(self):
        if self.study is None:
            self.study = optuna.create_study()

        if (not isinstance(self.concurrency, int)) and (self.concurrency < 0):
            raise ValueError("concurrency must be an integer greater than 0")

        if (not isinstance(self.n_trials, int)) and (self.n_trials < 0):
            raise ValueError("n_trials must be an integer greater than 0")

        if not isinstance(self.study, optuna.Study):
            raise ValueError("study must be an optuna.Study")

        # check if the objective function returns the correct number of outputs
        if isinstance(self.objective, PythonFunctionTask):
            func = self.objective.task_function
        elif isinstance(self.objective, PythonFunctionWorkflow):
            func = self.objective._workflow_function
        else:
            raise ValueError("objective must be a PythonFunctionTask or PythonFunctionWorkflow")

        signature = inspect.signature(func)

        if signature.return_annotation is float:
            if len(self.study.directions) != 1:
                raise ValueError("the study must have a single objective if objective returns a single float")

        elif isinstance(args := signature.return_annotation.__args__, tuple):
            if len(args) != len(self.study.directions):
                raise ValueError("objective must return the same number of directions in the study")

            if not all(arg is float for arg in args):
                raise ValueError("objective function must return a float or tuple of floats")

        else:
            raise ValueError("objective function must return a float or tuple of floats")

    async def __call__(self, suggestions: Optional[dict[str, Suggestion]] = None, /, **inputs: Any):
        """
        Asynchronously executes the objective function remotely.
        Parameters:
            suggestions (Optional[dict[str, Suggestion]]): bundled suggestions to objective function
            **inputs: other inputs to objective function
        """

        if suggestions is not None:
            if not isinstance(suggestions, dict):
                raise ValueError("suggestions must be a dict[str, Suggestion]")

            for key, value in suggestions.items():
                if not isinstance(key, str):
                    raise ValueError(f"suggestion key must be a string, got {type(key)}")
                if not isinstance(value, Suggestion):
                    raise ValueError(f"suggestion must be of type {type(value)}")

        # create semaphore to manage concurrency
        semaphore = asyncio.Semaphore(self.concurrency)

        # create list of async trials
        trials = [self.spawn(semaphore, suggestions, **inputs) for _ in range(self.n_trials)]

        # await all trials to complete
        await asyncio.gather(*trials)

    async def spawn(
        self,
        semaphore: asyncio.Semaphore,
        suggestions: Optional[dict[str, Suggestion]] = None,
        /,
        **inputs: Any,
    ):
        async with semaphore:
            # ask for a new trial
            trial: optuna.Trial = self.study.ask()

            if suggestions is not None:
                inputs["suggestions"] = self.suggest(trial, suggestions)

            inputs = self.suggest(trial, inputs)

            try:
                # schedule the trial
                result: Union[float, tuple[float, ...]] = await self.objective(**inputs)

                # tell the study the result
                self.study.tell(trial, result, state=optuna.trial.TrialState.COMPLETE)

            # if the trial fails, tell the study
            except EagerException:
                self.study.tell(trial, state=optuna.trial.TrialState.FAIL)

    @staticmethod
    def suggest(trial: optuna.Trial, inputs: dict[str, Any]) -> dict[str, Any]:
        suggesters = {
            Float: trial.suggest_float,
            Integer: trial.suggest_int,
            Category: trial.suggest_categorical,
        }

        for key, value in inputs.items():
            if isinstance(inputs[key], Suggestion):
                suggester = suggesters[type(value)]
                inputs[key] = suggester(name=key, **vars(value))

        return inputs

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Optional, Union

import flytekit as fl
import optuna
from flytekit.exceptions.eager import EagerException


class Suggestion:
    pass


@dataclass
class Float(Suggestion):
    low: float
    high: float
    step: Optional[float] = None
    log: bool = False


@dataclass
class Integer(Suggestion):
    low: int
    high: int
    step: int = 1
    log: bool = False


@dataclass
class Category(Suggestion):
    choices: list[Union[str, bool, int, None]] = field(default_factory=list)

    def __init__(self, *choices: Union[str, bool, int, None]):
        self.choices = list(choices)


suggest = SimpleNamespace(float=Float, integer=Integer, category=Category)


class FlyteExperiment:
    def __init__(
        self,
        concurrency: int,
        n_trials: int,
        objective: fl.PythonFunctionTask,
        study_name: str = "Flyte Study",
        directions: list[Union[str, optuna.study.StudyDirection]] = None,
        direction: Union[str, optuna.study.StudyDirection] = None,
        pruner: Optional[optuna.pruners.BasePruner] = None,
        sampler: Optional[optuna.samplers.BaseSampler] = None,
    ):

        if not objective.is_async:
            raise ValueError("Objective must be an asynchronous function")

        if not isinstance(concurrency, int):
            raise ValueError("Concurrency must be an integer")

        if not concurrency > 0:
            raise ValueError("Concurrency must be greater than 0")

        if not isinstance(n_trials, int):
            raise ValueError("n_trials must be an integer")

        if not n_trials > 0:
            raise ValueError("n_trials must be greater than 0")

        if not isinstance(objective, fl.PythonFunctionTask):
            raise ValueError("Objective must be a PythonFunctionTask")

        if not isinstance(study_name, str):
            raise ValueError("study_name must be a string")

        if directions is not None:
            if not isinstance(directions, list):
                raise ValueError("directions must be a list")
            if not all(isinstance(direction, (str, optuna.study.StudyDirection)) for direction in directions):
                raise ValueError("directions must be a list of strings or StudyDirections")

        if direction is not None:
            if not isinstance(direction, (str, optuna.study.StudyDirection)):
                raise ValueError("direction must be a string or StudyDirection")

        if pruner is not None:
            if not isinstance(pruner, optuna.pruners.BasePruner):
                raise ValueError("pruner must be a BasePruner")

        if sampler is not None:
            if not isinstance(sampler, optuna.samplers.BaseSampler):
                raise ValueError("sampler must be a BaseSampler")


        self.schema = {}
        self.objective = objective
        self.concurrency = concurrency
        self.n_trials = n_trials
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.is_multi_objective = directions is not None

        self.study = optuna.create_study(
            study_name=study_name,
            directions=directions,
            direction=direction,
            pruner=pruner,
            sampler=sampler,
        )

    def suggest(
        self, trial: optuna.Trial
    ) -> dict[str, Union[float, int, str, bool, None]]:

        suggestions = {}

        # specify trial method for each Suggestion type
        suggesters = {
            Float: trial.suggest_float,
            Integer: trial.suggest_int,
            Category: trial.suggest_categorical,
        }

        # suggest inputs for the trial
        for name, param in self.schema.items():
            suggester = suggesters[type(param)]

            suggestions[name] = suggester(name=name, **vars(param))

        return suggestions

    async def spawn(self, **fixed) -> None:

        async with self.semaphore:

            # ask for a new trial
            trial: optuna.Trial = self.study.ask()

            try:

                # suggest inputs for the trial
                inputs = self.suggest(trial)

                # update inputs with fixed values
                inputs.update(fixed)

                # schedule the trial
                result: Union[float, tuple[float, ...]] = await self.objective(**inputs)

                # tell the study the result
                self.study.tell(trial, result, state=optuna.trial.TrialState.COMPLETE)

            except EagerException:

                # if the trial fails, tell the study
                self.study.tell(trial, state=optuna.trial.TrialState.FAIL)

    async def __call__(self, **kwargs) -> None:

        # update schema with suggestions
        for key, value in kwargs.items():
            if isinstance(value, Suggestion):
                self.schema[key] = value

        # remove suggestions from kwargs
        for key in self.schema.keys():
            del kwargs[key]

        # create semaphore to manage concurrency
        self.semaphore = asyncio.Semaphore(self.concurrency)

        # create a list of trials to spawn
        promises = [self.spawn(**kwargs) for _ in range(self.n_trials)]

        # await all trials to complete
        await asyncio.gather(*promises)

        # reset schema and semaphore
        self.schema.clear()
        self.semaphore = None

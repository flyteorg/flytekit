import flytekit as fl
import pytest

from flytekitplugins.optuna import Optimizer


@fl.task
async def objective(x: float, y: int, z: int, power: int) -> float:
    return 1.0

def test_concurrency():

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=-1, n_trials=10)

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=0, n_trials=10)

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency="abc", n_trials=10)


def test_n_trials():

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=3, n_trials=-10)

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=3, n_trials=0)

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=3, n_trials="abc")



def test_study():

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=3, n_trials=10, study="abc")



def test_delay():

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=3, n_trials=10, delay=-1)

    with pytest.raises(ValueError):
        Optimizer(objective, concurrency=3, n_trials=10, delay="abc")



def test_objective():

    @fl.workflow
    def workflow(x: int, y: int, z: int, power: int) -> float:
        return 1.0


    with pytest.raises(ValueError):
        Optimizer(workflow, concurrency=3, n_trials=10)

    @fl.task
    async def mistyped_objective(x: int, y: int, z: int, power: int) -> str:
        return "abc"

    with pytest.raises(ValueError):
        Optimizer(mistyped_objective, concurrency=3, n_trials=10)

    @fl.task
    def synchronous_objective(x: int, y: int, z: int, power: int) -> float:
        return 1.0

    with pytest.raises(ValueError):
        Optimizer(synchronous_objective, concurrency=3, n_trials=10)



def test_callback():

    def callback(value: float):
        return 1.0

    with pytest.raises(ValueError):
        Optimizer(callback, concurrency=3, n_trials=10)

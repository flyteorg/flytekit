import asyncio
import math

import flytekit as fl
from flytekitplugins.optuna import Optimizer, suggest


image = fl.ImageSpec(builder="union", packages=["flytekit==1.15.0b0", "optuna>=4.0.0"])

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

    return optimizer.study.best_value()

def test_local_exec():

    loss = asyncio.run(train(concurrency=2, n_trials=10))

    assert isinstance(loss, float)

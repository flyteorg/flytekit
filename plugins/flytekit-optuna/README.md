# Fully Parallelized Flyte Orchestrated Optimizer

WIP Flyte integration with Optuna to parallelize optimization objective function runtime.

```python
import math

import flytekit as fl

from flytekitplugins.optuna import Optimizer, suggest

image = fl.ImageSpec(builder="union", packages=["flytekitplugins.optuna"])

@fl.task(container_image=image)
async def objective(x: float, y: int, z: int, power: int) -> float:
    return math.log((((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2)) ** power


@fl.eager(container_image=image)
async def train(concurrency: int, n_trials: int):
    optimizer = Optimizer(objective, concurrency, n_trials)

    await optimizer(
        x=suggest.float(low=-10, high=10),
        y=suggest.integer(low=-10, high=10),
        z=suggest.category([-5, 0, 3, 6, 9]),
        power=2,
    )
```

This integration allows one to define fully parallelized HPO experiments via `@eager` in as little as 20 lines of code. The objective task is optimized via Optuna under the hood, such that one may extract the `optuna.Study` at any time for the purposes of serialization, storage, visualization, or interpretation.

This plugin provides full feature parity to Optuna, including:

- fixed arguments
- multiple suggestion types (`Integer`, `Float`, `Category`)
- multi-objective, with arbitrary objective directions (minimize, maximize)
- pruners
- samplers

# Improvements

- This would synergize really well with Union Actors.
- This should also support workflows, but it currently does not.

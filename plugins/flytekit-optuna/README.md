# Flytekit Optuna Plugin

The Flytekit Optuna plugin provides the capability of scheduling a large set of concurrent Optuna Trials.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-optuna
```

```python
import math

import flytekit as fl

from experiment import FlyteExperiment, suggest

image = fl.ImageSpec(builder="union", packages=["flytekit==1.15.0b0", "optuna>=4.0.0"])


@fl.task(container_image=image)
async def objective(x: float, y: int, z: int, power: int) -> float:
    return math.log((((x - 5) ** 2) + (y + 4) ** 4 + (3 * z - 3) ** 2)) ** power


@fl.eager(container_image=image)
async def train(concurrency: int, n_trials: int):
    experiment = FlyteExperiment(
        concurrency=concurrency, n_trials=n_trials, objective=objective
    )

    await experiment(
        x=suggest.float(low=-10, high=10),
        y=suggest.integer(low=-10, high=10),
        z=suggest.category(-5, 0, 3, 6, 9),
        power=2,
    )
```

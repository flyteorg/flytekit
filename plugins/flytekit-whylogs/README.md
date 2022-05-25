# Flytekit whylogs Plugin

whylogs is an open source library for logging any kind of data. With whylogs,
you are able to generate summaries of datasets (called whylogs profiles) which
they can use to:

- Create data constraints to know whether your data looks the way it should
- Quickly visualize key summary statistics about a dataset
- Track changes in a dataset over time

```bash
pip install flytekitplugins-whylogs
```

To generate profiles, you can add a task like the following:

```python
from whylogs.core import DatasetProfileView
import whylogs as ylog

import pandas as pd

@task
def profile(df: pd.DataFrame) -> DatasetProfileView:
    result = ylog.log(df) # Various overloads for different common data types exist
    profile = result.profile().view()
    return profile
```

Note, you'll be passing around `DatasetProfileView` from tasks, not `DatasetProfile`.

## Validating Data

A common step in data pipelines is data validation. This can be done in
`whylogs` through the constraint feature. You'll be able to create failure tasks
if the data in the workflow doesn't conform to some configured constraints, like
min/max values on features, data types on features, etc.

```python
# TODO add constraint example once we release our constraint feature in 1.0.0
```

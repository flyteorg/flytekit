# Flytekit whylogs Plugin

whylogs is an open source library for logging any kind of data. With whylogs,
you are able to generate summaries of datasets (called whylogs profiles) which
can be used to:

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
    profile = result.view()
    return profile
```

>**NOTE:** You'll be passing around `DatasetProfileView` from tasks, not `DatasetProfile`.

## Validating Data

A common step in data pipelines is data validation. This can be done in
`whylogs` through the constraint feature. You'll be able to create failure tasks
if the data in the workflow doesn't conform to some configured constraints, like
min/max values on features, data types on features, etc.

```python
@task
def validate_data(profile: DatasetProfileView):
    column = profile.get_column("my_column")
    print(column.to_summary_dict()) # To see available things you can validate against
    builder = ConstraintsBuilder(profile)
    numConstraint = MetricConstraint(
        name='numbers between 0 and 4 only',
        condition=lambda x: x.min > 0 and x.max < 4,
        metric_selector=MetricsSelector(metric_name='distribution', column_name='my_column'))
    builder.add_constraint(numConstraint)
    constraint = builder.build()
    valid = constraint.validate()

    if(not valid):
        raise Exception("Invalid data found")
```

Check out our [constraints notebook](https://github.com/whylabs/whylogs/blob/1.0.x/python/examples/basic/MetricConstraints.ipynb) for more examples.

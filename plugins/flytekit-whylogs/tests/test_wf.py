from typing import List
import numpy as np
import pandas as pd
import whylogs as ylog
# TODO Loads the type transformer implicitly, but should I have to?
from flytekitplugins.whylogs import WhylogsDatasetProfileTransformer
from whylogs.core import DatasetProfileView

from whylogs.core.constraints import Constraints, ConstraintsBuilder, MetricsSelector, MetricConstraint


from flytekit import task, workflow


@task
def make_data(numbers: List[int]) -> pd.DataFrame:
    data = {'Letters': ['a', 'b', 'c', 'c'],
            'Num': numbers}
    return pd.DataFrame(data)


@task
def profile(df: pd.DataFrame) -> DatasetProfileView:
    result = ylog.log(df)
    return result.profile().view()


@task
def constraints(profile: DatasetProfileView):
    builder = ConstraintsBuilder(profile)
    numConstraint = MetricConstraint(
        name='numbers between 0 and 4 only',
        condition=lambda x: x.min > 0 and x.max < 4,
        metric_selector=MetricsSelector(metric_name='distribution', column_name='Num'))
    builder.add_constraint(numConstraint)
    constraint = builder.build()
    valid = constraint.validate()

    if(not valid):
        raise Exception("Invalid data found")


@workflow
def wf(numbers: List[int]) -> DatasetProfileView:
    df = make_data(numbers=numbers)
    p = profile(df=df)
    constraints(profile=p)
    return p


def test_workflow_with_whylogs():
    profile = wf(numbers=[1, 2, 3, 3])
    keys = list(profile.get_columns().keys())
    assert keys == ['Letters', 'Num']


def test_constraints():
    try:
        profile = wf(numbers=[-2, 0, 0, 10])
        assert False
    except Exception:
        # Should have an error because constraints are violated
        assert True


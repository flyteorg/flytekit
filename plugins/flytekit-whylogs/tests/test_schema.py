from typing import Optional, Tuple
import unittest

import plotly.express as px
import numpy as np
import pandas as pd
import whylogs as why
from whylogs.core import DatasetProfileView
from whylogs.core.constraints import ConstraintsBuilder, MetricsSelector, MetricConstraint

import flytekit
from flytekit import task, workflow
from flytekitplugins.deck.renderer import WhylogsSummaryDriftRenderer, WhylogsConstraintsRenderer


@task
def read_data() -> pd.DataFrame:
    return px.data.iris()


@task
def make_data(n_rows: int = 3) -> pd.DataFrame:
    data = {
        'sepal_length': np.random.random_sample(n_rows),
        'sepal_width': np.random.random_sample(n_rows),
        'petal_length': np.random.random_sample(n_rows),
        'petal_width': np.random.random_sample(n_rows),
        'species': np.random.choice(['virginica', 'setosa', 'versicolor'], n_rows),
        'species_id': np.random.choice([1, 2, 3], n_rows)
    }
    return pd.DataFrame(data)


@task
def profile_data(df: pd.DataFrame) -> DatasetProfileView:
    result = why.log(df)
    return result.view()


@task
def summary_drift_report(target_profile_view: DatasetProfileView, ref_profile_view: DatasetProfileView) -> DatasetProfileView:
    renderer = WhylogsSummaryDriftRenderer()
    flytekit.Deck("summary drift", renderer.to_html(target_view=target_profile_view, reference_view=ref_profile_view))
    return target_profile_view


@task
def run_constraints(profile_view: DatasetProfileView,
                    min_value: Optional[float] = 0.0,
                    max_value: Optional[float] = 4.0
                    ) -> bool:
    # This API constraints workflow is flexible but a bit cumbersome.
    # It will be simplified in the future.
    builder = ConstraintsBuilder(profile_view)
    num_constraint = MetricConstraint(
        name=f'numbers between {min_value} and {max_value} only',
        condition=lambda x: x.min > min_value and x.max < max_value,
        metric_selector=MetricsSelector(metric_name='distribution', column_name='sepal_length'))
    builder.add_constraint(num_constraint)
    constraints = builder.build()

    renderer = WhylogsConstraintsRenderer()
    flytekit.Deck("constraints", renderer.to_html(constraints=constraints))

    return constraints.validate()


@workflow
def whylogs_workflow(min_value: float, max_value: float) -> Tuple[DatasetProfileView, bool]:
    new_data = make_data(n_rows=10)
    reference_data = read_data()

    new_profile = profile_data(df=new_data)
    ref_profile = profile_data(df=reference_data)

    summary_drift_report(target_profile_view=new_profile, ref_profile_view=ref_profile)
    validated = run_constraints(profile_view=new_profile, min_value=min_value, max_value=max_value)
    return new_profile, validated


class TestWhylogsWorkflow(unittest.TestCase):
    def test_workflow_with_whylogs(self):
        profile_view, _ = whylogs_workflow(min_value=0.0, max_value=1.0)
        keys = list(profile_view.get_columns().keys())
        assert keys.sort() == [
            'sepal_length',
            'sepal_width',
            'petal_length',
            'petal_width',
            'species',
            'species_id'
        ].sort()

    def test_constraints(self):
        _, validated = whylogs_workflow(min_value=-1.0, max_value=0.0)
        assert validated is False

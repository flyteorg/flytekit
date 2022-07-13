from typing import Optional
import unittest

import plotly.express as px
import numpy as np
import pandas as pd
import pyspark
import whylogs as why
from whylogs.core import DatasetProfileView
from whylogs.core.constraints import ConstraintsBuilder, MetricsSelector, MetricConstraint
from whylogs.api.pyspark.experimental.profiler import collect_dataset_profile_view

import flytekit
from flytekitplugins.spark import Spark
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
def summary_drift_report(target_data: pd.DataFrame, reference_data: pd.DataFrame) -> None:
    renderer = WhylogsSummaryDriftRenderer()
    flytekit.Deck("summary drift", renderer.to_html(target_data=target_data, reference_data=reference_data))

@task
def run_constraints(df: pd.DataFrame,
                    min_value: Optional[float] = 0.0,
                    max_value: Optional[float] = 4.0
                    ) -> bool:
    # This API constraints workflow is very flexible but a bit cumbersome.
    # It will be simplified in the future, so for now we'll stick with injecting
    # a Constraints object to the renderer.
    profile_view = why.log(df).view()
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
def whylogs_workflow(min_value: float, max_value: float) -> bool:
    new_data = make_data(n_rows=10)
    reference_data = read_data()

    summary_drift_report(target_data=new_data, reference_data=reference_data)
    validated = run_constraints(df=new_data, min_value=min_value, max_value=max_value)
    return validated


@task(
    task_config=Spark(
        spark_conf={
            "spark.driver.memory": "1000M",
            "spark.executor.instances": "2",
            "spark.driver.cores": "1",
        }
    ),
    cache_version="1",
    cache=True,
)
def make_spark_dataframe(n_rows: int = 10) -> pyspark.sql.DataFrame:
    spark = flytekit.current_context().spark_session
    data = {
        'sepal_length': np.random.random_sample(n_rows),
        'sepal_width': np.random.random_sample(n_rows),
        'petal_length': np.random.random_sample(n_rows),
        'petal_width': np.random.random_sample(n_rows),
        'species': np.random.choice(['virginica', 'setosa', 'versicolor'], n_rows),
        'species_id': np.random.choice([1, 2, 3], n_rows)
    }
    pandas_df = pd.DataFrame(data)
    spark_df = spark.createDataFrame(pandas_df)
    return spark_df

@task(
    task_config=Spark(
        spark_conf={
            "spark.driver.memory": "1000M",
            "spark.executor.instances": "2",
            "spark.driver.cores": "1",
        }
    ),
    cache_version="1",
    cache=True,
)
def profile_spark_df(df: pyspark.sql.DataFrame) -> DatasetProfileView:
    profile_view = collect_dataset_profile_view(df)
    return profile_view


@workflow
def spark_whylogs_wf() -> DatasetProfileView:
    spark_df = make_spark_dataframe(n_rows=10)
    profile_view = profile_spark_df(df=spark_df)
    return profile_view



class TestWhylogsWorkflow(unittest.TestCase):
    def test_workflow_with_whylogs(self):
        validated = whylogs_workflow(min_value=0.0, max_value=1.0)
        assert validated is True

    def test_constraints(self):
        validated = whylogs_workflow(min_value=-1.0, max_value=0.0)
        assert validated is False

    def test_pyspark_wf(self):
        profile_view = spark_whylogs_wf()
        assert profile_view is not None
        assert isinstance(profile_view, DatasetProfileView)
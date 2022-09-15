import math
import typing
from functools import wraps, partial

import mlflow
import pandas
import pandas as pd
import plotly.graph_objects as go
from mlflow import MlflowClient
from mlflow.entities.metric import Metric
from plotly.subplots import make_subplots

import flytekit


def metric_to_df(metrics: typing.List[Metric]) -> pd.DataFrame:
    """
    Converts mlflow Metric object to a dataframe of 2 columns ['timestamp', 'value']
    """
    t = []
    v = []
    for m in metrics:
        t.append(m.timestamp)
        v.append(m.value)
    return pd.DataFrame(list(zip(t, v)), columns =['timestamp', 'value'])


def get_run_metrics(c: MlflowClient, run_id: str) -> typing.Dict[str, pandas.DataFrame]:
    """
    Extracts all metrics and returns a dictionary of metric name to the list of metric for the given run_id
    """
    r = c.get_run(run_id)
    metrics = {}
    for k in r.data.metrics.keys():
        metrics[k] = metric_to_df(metrics=c.get_metric_history(run_id=run_id, key=k))
    return metrics


def plot_metrics(metrics: typing.Dict[str, pandas.DataFrame]) -> go.Figure:
    v = len(metrics)
    # Initialize figure with subplots
    rows = math.ceil(v/3)
    cols = 3
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=list(metrics.keys()))

    # Add traces
    row=1
    col=1
    for k, v in metrics.items():
        fig.add_trace(go.Scatter(x=v['timestamp'], y=v['value'], name=k), row=row, col=col)
        col = col + 1
        if col > cols:
            col = 1
            row = row + 1

    fig.update_layout(height=rows*300)
    return fig


def mlflow_autolog(fn=None, *, framework=mlflow.sklearn):
    """
    This decorator can be used as a nested decorator for a ``@task`` and it will automatically enable mlflow autologging,
    for the given ``framework``. If framework is not provided then the autologging is enabled for ``sklearn``

    .. code-block::python

        @task
        @mlflow_autlog(framework=mlflow.tensorflow)
        def my_tensorflow_trainer():
            ...

    One benefit of doing so is that the mlflow metrics are then rendered inline using FlyteDecks and can be viewed
    in jupyter notebook, as well as in hosted Flyte environment

    .. code-block:: python

        with flytekit.new_context() as ctx:
            my_tensorflow_trainer()
            ctx.get_deck()  # IPython.display

    The decorator starts a new run, with mlflow for the task
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        framework.autolog()
        with mlflow.start_run():
            out = fn(*args, **kwargs)
            run = mlflow.active_run()
            if run is not None:
                c = MlflowClient()
                m = get_run_metrics(c, run.info.run_id)
                f = plot_metrics(m)
                flytekit.current_context().default_deck.append(f.to_html())
        return out

    if fn is None:
        return partial(mlflow_autolog, framework=framework)

    return wrapper

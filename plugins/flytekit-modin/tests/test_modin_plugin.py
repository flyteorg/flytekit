import os

import ray
from flytekitplugins.modin import schema  # noqa F401
from modin import pandas as pd

from flytekit import task, workflow

os.environ["MODIN_ENGINE"] = "ray"
if not ray.is_initialized():
    ray.init(_plasma_directory="/tmp")  # setting to disable out of core in Ray


def test_modin_workflow(data="https://modin-datasets.s3.amazonaws.com/plasticc/training_set.csv"):
    @task
    def generate(df_path: str) -> pd.DataFrame:
        df = pd.read_csv(df_path)
        return df

    @task
    def consume(d: pd.DataFrame) -> pd.DataFrame:
        df = d
        return df.head()

    @workflow
    def wf(df_path: str = data) -> pd.DataFrame:
        return consume(d=generate(df_path=df_path))

    result = wf()
    assert result is not None
    assert isinstance(result, pd.DataFrame)

import os

import ray

os.environ["MODIN_ENGINE"] = "ray"
if not ray.is_initialized():
    ray.init(include_dashboard=True, _plasma_directory="/tmp")  # setting to disable out of core in Ray

from flytekitplugins.modin import schema  # noqa: F401 E402
from modin import pandas  # noqa: E402

from flytekit import task, workflow  # noqa: E402


@task
def generate(df_path: str) -> pandas.DataFrame:
    df = pandas.read_csv(df_path)
    return df


@task
def consume(d: pandas.DataFrame) -> pandas.DataFrame:
    df = d
    return df.head()


@workflow
def wf(df_path: str = "https://modin-datasets.s3.amazonaws.com/trips_data.csv") -> pandas.DataFrame:
    return consume(d=generate(df_path))


result = wf()
assert isinstance(result, pandas.DataFrame)

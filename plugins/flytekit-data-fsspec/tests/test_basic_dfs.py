import pandas as pd
import pyarrow as pa
from flytekitplugins.fsspec.pandas import get_storage_options

from flytekit import kwtypes, task
from flytekit.configuration import aws

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated


def test_get_storage_options():
    endpoint = "https://s3.amazonaws.com"
    with aws.S3_ENDPOINT.get_patcher(endpoint):
        options = get_storage_options("s3://bucket/somewhere")
        assert options == {"client_kwargs": {"endpoint_url": endpoint}}

    options = get_storage_options("/tmp/file")
    assert options is None


cols = kwtypes(Name=str, Age=int)
subset_cols = kwtypes(Name=str)


@task
def t1(
    df1: Annotated[pd.DataFrame, cols], df2: Annotated[pa.Table, cols]
) -> (Annotated[pd.DataFrame, subset_cols], Annotated[pa.Table, subset_cols]):
    return df1, df2


def test_structured_dataset_wf():
    pd_df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})
    pa_df = pa.Table.from_pandas(pd_df)

    subset_pd_df = pd.DataFrame({"Name": ["Tom", "Joseph"]})
    subset_pa_df = pa.Table.from_pandas(subset_pd_df)

    df1, df2 = t1(df1=pd_df, df2=pa_df)
    assert df1.equals(subset_pd_df)
    assert df2.equals(subset_pa_df)

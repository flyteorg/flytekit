import typing

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from flytekit import FlyteContext, kwtypes, task, workflow
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.types.structured.structured_dataset import (
    DF,
    FLYTE_DATASET_TRANSFORMER,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)
from flytekit.types.structured.utils import get_filesystem

PANDAS_PATH = "/tmp/pandas.pq"
NUMPY_PATH = "/tmp/numpy.pq"
BQ_PATH = "bq://photo-313016:flyte.new_table3"

# https://github.com/flyteorg/flyte/issues/523
# We should also support map and list in schema column
my_cols = kwtypes(w=typing.Dict[str, typing.Dict[str, int]], x=typing.List[typing.List[int]], y=int, z=str)


@task
def t1(dataframe: pd.DataFrame) -> pd.DataFrame:
    # S3 (parquet) -> Pandas -> S3 (parquet) default behaviour
    return dataframe


@task
def t1a(dataframe: pd.DataFrame) -> StructuredDataset[my_cols]:
    # S3 (parquet) -> Pandas -> S3 (parquet) default behaviour
    return StructuredDataset(dataframe=dataframe, uri=PANDAS_PATH)


@task
def t2(dataframe: pd.DataFrame) -> pd.DataFrame:
    # S3 (parquet) -> Pandas -> S3 (parquet)
    return dataframe


@task
def t3(dataset: StructuredDataset[my_cols]) -> StructuredDataset[my_cols]:
    # s3 (parquet) -> pandas -> s3 (parquet)
    print("Pandas dataframe")
    print(dataset.open_as(pd.DataFrame))
    # In the example, we download dataset when we open it.
    # Here we won't upload anything, since we're returning just the input object.
    return dataset


@task
def t3a(dataset: StructuredDataset[my_cols]) -> StructuredDataset[my_cols]:
    # This task will not do anything - no uploading, no downloading
    return dataset


@task
def t4(dataset: StructuredDataset[my_cols]) -> pd.DataFrame:
    # s3 (parquet) -> pandas -> s3 (parquet)
    return dataset.open_as(pd.DataFrame)


@task
def t5(dataframe: pd.DataFrame) -> StructuredDataset[my_cols]:
    # s3 (parquet) -> pandas -> bq
    return StructuredDataset(dataframe=dataframe, uri=BQ_PATH)


@task
def t6(dataset: StructuredDataset[my_cols]) -> pd.DataFrame:
    # bq -> pandas -> s3 (parquet)
    df = dataset.open_as(pd.DataFrame)
    return df


@task
def t7(df1: pd.DataFrame, df2: pd.DataFrame) -> (StructuredDataset[my_cols], StructuredDataset[my_cols]):
    # df1: pandas -> bq
    # df2: pandas -> s3 (parquet)
    return StructuredDataset(dataframe=df1, uri=BQ_PATH), StructuredDataset(dataframe=df1)


@task
def t8(dataframe: pa.Table) -> StructuredDataset[my_cols]:
    # Arrow table -> s3 (parquet)
    print("Arrow table")
    print(dataframe.columns)
    return StructuredDataset(dataframe=dataframe, uri=PANDAS_PATH)


@task
def t8a(dataframe: pa.Table) -> pa.Table:
    # Arrow table -> s3 (parquet)
    print(dataframe.columns)
    return dataframe


class NumpyEncodingHandlers(StructuredDatasetEncoder):
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
    ) -> literals.StructuredDataset:
        path = typing.cast(str, structured_dataset.uri)
        df = structured_dataset.dataframe
        name = ["col" + str(i) for i in range(len(df))]
        table = pa.Table.from_arrays(df, name)
        pq.write_table(table, path, filesystem=get_filesystem(path))
        return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(format="parquet"))


class NumpyDecodingHandlers(StructuredDatasetDecoder):
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> typing.Union[DF, typing.Generator[DF, None, None]]:
        path = flyte_value.uri
        table = pq.read_table(path, filesystem=get_filesystem(path))
        return table.to_pandas().to_numpy()


FLYTE_DATASET_TRANSFORMER.register_handler(NumpyEncodingHandlers(np.ndarray, "local", "parquet"))
FLYTE_DATASET_TRANSFORMER.register_handler(NumpyDecodingHandlers(np.ndarray, "local", "parquet"))


@task
def t9(dataframe: np.ndarray) -> StructuredDataset[my_cols]:
    # numpy -> Arrow table -> s3 (parquet)
    return StructuredDataset(dataframe=dataframe, uri=NUMPY_PATH)


@task
def t10(dataset: StructuredDataset[my_cols]) -> np.ndarray:
    # s3 (parquet) -> Arrow table -> numpy
    np_array = dataset.open_as(np.ndarray)
    return np_array


@task
def generate_pandas() -> pd.DataFrame:
    return pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})


@task
def generate_numpy() -> np.ndarray:
    return np.array([[1, 2], [4, 5]])


@task
def generate_arrow() -> pa.Table:
    return pa.Table.from_pandas(pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]}))


@workflow()
def wf():
    df = generate_pandas()
    np_array = generate_numpy()
    arrow_df = generate_arrow()
    t1(dataframe=df)
    t1a(dataframe=df)
    t2(dataframe=df)
    t3(dataset=StructuredDataset(uri=PANDAS_PATH))
    t3a(dataset=StructuredDataset(uri=PANDAS_PATH))
    t4(dataset=StructuredDataset(uri=PANDAS_PATH))
    t5(dataframe=df)
    t6(dataset=StructuredDataset(uri=BQ_PATH))
    t7(df1=df, df2=df)
    t8(dataframe=arrow_df)
    t8a(dataframe=arrow_df)
    t9(dataframe=np_array)
    t10(dataset=StructuredDataset(uri=NUMPY_PATH))


if __name__ == "__main__":
    wf()

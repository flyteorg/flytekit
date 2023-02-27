from typing import List, Union

import pandas as pd
import pyarrow as pa
from flytekitplugins.duckdb import DuckDBQuery
from typing_extensions import Annotated

from flytekit import kwtypes, task, workflow
from flytekit.types.structured.structured_dataset import StructuredDataset


def test_simple():
    duckdb_task = DuckDBQuery(name="duckdb_task", query="SELECT SUM(a) FROM mydf", inputs=kwtypes(mydf=pd.DataFrame))

    @workflow
    def pandas_wf(mydf: pd.DataFrame) -> pd.DataFrame:
        return duckdb_task(mydf=mydf)

    @workflow
    def arrow_wf(mydf: pd.DataFrame) -> pa.Table:
        return duckdb_task(mydf=mydf)

    df = pd.DataFrame({"a": [1, 2, 3]})
    assert isinstance(pandas_wf(mydf=df), pd.DataFrame)
    assert isinstance(arrow_wf(mydf=df), pa.Table)


def test_parquet():
    duckdb_query = DuckDBQuery(
        name="read_parquet",
        query=[
            "INSTALL httpfs",
            "LOAD httpfs",
            """SELECT hour(lpep_pickup_datetime) AS hour, count(*) AS count FROM READ_PARQUET(?) GROUP BY hour""",
        ],
        inputs=kwtypes(params=List[str]),
    )

    @workflow
    def parquet_wf(parquet_file: str) -> pd.DataFrame:
        return duckdb_query(params=[parquet_file])

    assert isinstance(
        parquet_wf(parquet_file="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet"),
        pd.DataFrame,
    )


def test_arrow():
    duckdb_task = DuckDBQuery(
        name="duckdb_arrow_task", query="SELECT * FROM arrow_table WHERE i = 2", inputs=kwtypes(arrow_table=pa.Table)
    )

    @task
    def get_arrow_table() -> pa.Table:
        return pa.Table.from_pydict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})

    @workflow
    def arrow_wf(arrow_table: pa.Table) -> pa.Table:
        return duckdb_task(arrow_table=arrow_table)

    assert isinstance(arrow_wf(arrow_table=get_arrow_table()), pa.Table)


def test_structured_dataset_arrow_table():
    duckdb_task = DuckDBQuery(
        name="duckdb_sd_table",
        query="SELECT * FROM arrow_table WHERE i = 2",
        inputs=kwtypes(arrow_table=StructuredDataset),
    )

    @task
    def get_arrow_table() -> StructuredDataset:
        return StructuredDataset(
            dataframe=pa.Table.from_pydict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        )

    @workflow
    def arrow_wf(arrow_table: StructuredDataset) -> pa.Table:
        return duckdb_task(arrow_table=arrow_table)

    assert isinstance(arrow_wf(arrow_table=get_arrow_table()), pa.Table)


def test_structured_dataset_pandas_df():
    duckdb_task = DuckDBQuery(
        name="duckdb_sd_df",
        query="SELECT * FROM pandas_df WHERE i = 2",
        inputs=kwtypes(pandas_df=StructuredDataset),
    )

    @task
    def get_pandas_df() -> StructuredDataset:
        return StructuredDataset(
            dataframe=pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
        )

    @workflow
    def pandas_wf(pandas_df: StructuredDataset) -> pd.DataFrame:
        return duckdb_task(pandas_df=pandas_df)

    assert isinstance(pandas_wf(pandas_df=get_pandas_df()), pd.DataFrame)


def test_distinct_params():
    duckdb_params_query = DuckDBQuery(
        name="params_query",
        query=[
            "CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)",
            "INSERT INTO items VALUES (?, ?, ?)",
            "SELECT $1 AS one, $1 AS two, $2 AS three",
        ],
        inputs=kwtypes(params=List[List[Union[str, List[Union[str, int]]]]]),
    )

    @task
    def read_df(df: Annotated[StructuredDataset, kwtypes(one=str)]) -> pd.DataFrame:
        return df.open(pd.DataFrame).all()

    @workflow
    def params_wf(params: List[List[Union[str, List[Union[str, int]]]]]) -> pd.DataFrame:
        return read_df(df=duckdb_params_query(params=params))

    params = [[["chainsaw", 500, 10], ["iphone", 300, 2]], ["duck", "goose"]]
    wf_output = params_wf(params=params)
    assert isinstance(wf_output, pd.DataFrame)
    assert wf_output.columns.values == ["one"]


def test_insert_query_with_single_params():
    duckdb_params_query = DuckDBQuery(
        name="params_query",
        query=[
            "CREATE TABLE items(value DECIMAL(10,2))",
            "INSERT INTO items VALUES (?)",
            "SELECT * FROM items",
        ],
        inputs=kwtypes(params=List[List[List[int]]]),
    )

    @workflow
    def params_wf(params: List[List[List[int]]]) -> pa.Table:
        return duckdb_params_query(params=params)

    assert isinstance(params_wf(params=[[[500], [300], [2]]]), pa.Table)

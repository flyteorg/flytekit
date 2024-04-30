import os
import typing
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typing_extensions import Annotated

from flytekit import FlyteContext, FlyteContextManager, StructuredDataset, kwtypes, task, workflow
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.basic_dfs import CSVToPandasDecodingHandler, PandasToCSVEncodingHandler
from flytekit.types.structured.structured_dataset import (
    DF,
    PARQUET,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)

pd = pytest.importorskip("pandas")

PANDAS_PATH = FlyteContextManager.current_context().file_access.get_random_local_directory()
NUMPY_PATH = FlyteContextManager.current_context().file_access.get_random_local_directory()
BQ_PATH = "bq://flyte-dataset:flyte.table"


@dataclass
class MyCols:
    Name: str
    Age: int


my_cols = kwtypes(Name=str, Age=int)
my_dataclass_cols = MyCols
my_dict_cols = {"Name": str, "Age": int}
fields = [("Name", pa.string()), ("Age", pa.int32())]
arrow_schema = pa.schema(fields)
pd_df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})


class MockBQEncodingHandlers(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pd.DataFrame, "bq", "")

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        return literals.StructuredDataset(
            uri="bq://bucket/key", metadata=StructuredDatasetMetadata(structured_dataset_type)
        )


class MockBQDecodingHandlers(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pd.DataFrame, "bq", "")

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
        return pd_df


StructuredDatasetTransformerEngine.register(MockBQEncodingHandlers(), False, True)
StructuredDatasetTransformerEngine.register(MockBQDecodingHandlers(), False, True)


class NumpyRenderer:
    """
    The Polars DataFrame summary statistics are rendered as an HTML table.
    """

    def to_html(self, array: np.ndarray) -> str:
        return pd.DataFrame(array).describe().to_html()


@pytest.fixture(autouse=True)
def numpy_type():
    class NumpyEncodingHandlers(StructuredDatasetEncoder):
        def encode(
            self,
            ctx: FlyteContext,
            structured_dataset: StructuredDataset,
            structured_dataset_type: StructuredDatasetType,
        ) -> literals.StructuredDataset:
            path = typing.cast(str, structured_dataset.uri)
            if not path:
                path = ctx.file_access.join(
                    ctx.file_access.raw_output_prefix,
                    ctx.file_access.get_random_string(),
                )
            df = typing.cast(np.ndarray, structured_dataset.dataframe)
            name = ["col" + str(i) for i in range(len(df))]
            table = pa.Table.from_arrays(df, name)
            local_dir = ctx.file_access.get_random_local_directory()
            local_path = os.path.join(local_dir, f"{0:05}")
            pq.write_table(table, local_path)
            ctx.file_access.upload_directory(local_dir, path)
            structured_dataset_type.format = PARQUET
            return literals.StructuredDataset(uri=path, metadata=StructuredDatasetMetadata(structured_dataset_type))

    class NumpyDecodingHandlers(StructuredDatasetDecoder):
        def decode(
            self,
            ctx: FlyteContext,
            flyte_value: literals.StructuredDataset,
            current_task_metadata: StructuredDatasetMetadata,
        ) -> typing.Union[DF, typing.Generator[DF, None, None]]:
            path = flyte_value.uri
            local_dir = ctx.file_access.get_random_local_directory()
            ctx.file_access.get_data(path, local_dir, is_multipart=True)
            table = pq.read_table(local_dir)
            return table.to_pandas().to_numpy()

    StructuredDatasetTransformerEngine.register(NumpyEncodingHandlers(np.ndarray))
    StructuredDatasetTransformerEngine.register(NumpyDecodingHandlers(np.ndarray))
    StructuredDatasetTransformerEngine.register_renderer(np.ndarray, NumpyRenderer())


StructuredDatasetTransformerEngine.register(PandasToCSVEncodingHandler())
StructuredDatasetTransformerEngine.register(CSVToPandasDecodingHandler())


## Case 1.
data = [
    {
        "company": "XYZ pvt ltd",
        "location": "London",
        "info": {"president": "Rakesh Kapoor", "contacts": {"email": "contact@xyz.com", "tel": "9876543210"}},
    },
    {
        "company": "ABC pvt ltd",
        "location": "USA",
        "info": {"president": "Kapoor Rakesh", "contacts": {"email": "contact@abc.com", "tel": "0123456789"}},
    },
]


@dataclass
class ContactsField:
    email: str
    tel: str


@dataclass
class InfoField:
    president: str
    contacts: ContactsField


@dataclass
class CompanyField:
    location: str
    info: InfoField
    company: str


MyArgDataset = Annotated[StructuredDataset, kwtypes(company=str)]
MyDictDataset = Annotated[StructuredDataset, kwtypes(info={"contacts": {"tel": str}})]
MyDictListDataset = Annotated[StructuredDataset, kwtypes(info={"contacts": {"tel": str, "email": str}})]
MyTopDataClassDataset = Annotated[StructuredDataset, CompanyField]
MyTopDictDataset = Annotated[StructuredDataset, {"company": str, "location": str}]
MySecondDataClassDataset = Annotated[StructuredDataset, kwtypes(info=InfoField)]
MyNestedDataClassDataset = Annotated[StructuredDataset, kwtypes(info=kwtypes(contacts=ContactsField))]


@task()
def create_pd_table() -> StructuredDataset:
    df = pd.json_normalize(data, max_level=0)
    print("original dataframe: \n", df)

    return StructuredDataset(dataframe=df, uri=PANDAS_PATH)


@task()
def create_bq_table() -> StructuredDataset:
    df = pd.json_normalize(data, max_level=0)
    print("original dataframe: \n", df)

    # Enable one of GCP `uri` below if you want. You can replace `uri` with your own google cloud endpoints.
    return StructuredDataset(dataframe=df, uri=BQ_PATH)


@task()
def create_np_table() -> StructuredDataset:
    df = pd.json_normalize(data, max_level=0)
    print("original dataframe: \n", df)

    return StructuredDataset(dataframe=df, uri=NUMPY_PATH)


@task()
def create_ar_table() -> StructuredDataset:
    df = pa.Table.from_pandas(pd.json_normalize(data, max_level=0))
    print("original dataframe: \n", df)

    return StructuredDataset(
        dataframe=df,
    )


@task()
def print_table_by_arg(sd: MyArgDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyArgDataset dataframe: \n", t)
    return t


@task()
def print_table_by_dict(sd: MyDictDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyDictDataset dataframe: \n", t)
    return t


@task()
def print_table_by_list_dict(sd: MyDictListDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyDictListDataset dataframe: \n", t)
    return t


@task()
def print_table_by_top_dataclass(sd: MyTopDataClassDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyTopDataClassDataset dataframe: \n", t)
    return t


@task()
def print_table_by_top_dict(sd: MyTopDictDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyTopDictDataset dataframe: \n", t)
    return t


@task()
def print_table_by_second_dataclass(sd: MySecondDataClassDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MySecondDataClassDataset dataframe: \n", t)
    return t


@task()
def print_table_by_nested_dataclass(sd: MyNestedDataClassDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyNestedDataClassDataset dataframe: \n", t)
    return t


@workflow
def wf():
    pd_sd = create_pd_table()
    print_table_by_arg(sd=pd_sd)
    print_table_by_dict(sd=pd_sd)
    print_table_by_list_dict(sd=pd_sd)
    print_table_by_top_dataclass(sd=pd_sd)
    print_table_by_top_dict(sd=pd_sd)
    print_table_by_second_dataclass(sd=pd_sd)
    print_table_by_nested_dataclass(sd=pd_sd)
    bq_sd = create_pd_table()
    print_table_by_arg(sd=bq_sd)
    print_table_by_dict(sd=bq_sd)
    print_table_by_list_dict(sd=bq_sd)
    print_table_by_top_dataclass(sd=bq_sd)
    print_table_by_top_dict(sd=bq_sd)
    print_table_by_second_dataclass(sd=bq_sd)
    print_table_by_nested_dataclass(sd=bq_sd)
    np_sd = create_pd_table()
    print_table_by_arg(sd=np_sd)
    print_table_by_dict(sd=np_sd)
    print_table_by_list_dict(sd=np_sd)
    print_table_by_top_dataclass(sd=np_sd)
    print_table_by_top_dict(sd=np_sd)
    print_table_by_second_dataclass(sd=np_sd)
    print_table_by_nested_dataclass(sd=np_sd)
    ar_sd = create_pd_table()
    print_table_by_arg(sd=ar_sd)
    print_table_by_dict(sd=ar_sd)
    print_table_by_list_dict(sd=ar_sd)
    print_table_by_top_dataclass(sd=ar_sd)
    print_table_by_top_dict(sd=ar_sd)
    print_table_by_second_dataclass(sd=ar_sd)
    print_table_by_nested_dataclass(sd=ar_sd)


def test_structured_dataset_wf():
    wf()

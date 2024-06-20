from dataclasses import dataclass

import pyarrow as pa
import pytest
from typing_extensions import Annotated

from flytekit import FlyteContextManager, StructuredDataset, kwtypes, task, workflow

pd = pytest.importorskip("pandas")

PANDAS_PATH = FlyteContextManager.current_context().file_access.get_random_local_directory()
NUMPY_PATH = FlyteContextManager.current_context().file_access.get_random_local_directory()
BQ_PATH = "bq://flyte-dataset:flyte.table"


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

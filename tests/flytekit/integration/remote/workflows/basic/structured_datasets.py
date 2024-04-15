from dataclasses import dataclass
from typing import Annotated

import pandas as pd

from flytekit import ImageSpec, StructuredDataset, kwtypes, task, workflow

## Add `@task(container_image=image)` if want to test in remote mode. Remove `git_commit_sha` after merged.
## Add `GOOGLE_APPLICATION_CREDENTIALS` if wanna test `google-cloud-bigquery`.
flytekit_dev_version = "https://github.com/austin362667/flytekit.git@f5cd70dd053e6f3d4aaf5b90d9c4b28f32c0980a"
image = ImageSpec(
    packages=[
        "pandas",
        # "google-cloud-bigquery",
        # "google-cloud-bigquery-storage",
        # "flytekitplugins-bigquery==1.11.0",
        f"git+{flytekit_dev_version}",
    ],
    apt_packages=["git"],
    # source_root="./keys",
    # env={"GOOGLE_APPLICATION_CREDENTIALS": "./gcp-service-account.json"},
    platform="linux/arm64",
    registry="localhost:30000",
)


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
MyTopDataClassDataset = Annotated[StructuredDataset, kwtypes(CompanyField)]
MySecondDataClassDataset = Annotated[StructuredDataset, kwtypes(info=InfoField)]
MyNestedDataClassDataset = Annotated[StructuredDataset, kwtypes(info=kwtypes(contacts=ContactsField))]


@task(container_image=image)
def create_bq_table() -> StructuredDataset:
    df = pd.json_normalize(data, max_level=0)
    print("original dataframe: \n", df)

    # Enable one of GCP `uri` below if you want. You can replace `uri` with your own google cloud endpoints.
    return StructuredDataset(
        dataframe=df,
        # uri= "gs://flyte_austin362667_bucket/nested_types"
        # uri= "bq://flyte-austin362667-gcp:dataset.nested_type"
    )


@task(container_image=image)
def print_table_by_arg(sd: MyArgDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyArgDataset dataframe: \n", t)
    return t


@task(container_image=image)
def print_table_by_dict(sd: MyDictDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyDictDataset dataframe: \n", t)
    return t


@task(container_image=image)
def print_table_by_list_dict(sd: MyDictListDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyDictListDataset dataframe: \n", t)
    return t


@task(container_image=image)
def print_table_by_top_dataclass(sd: MyTopDataClassDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyTopDataClassDataset dataframe: \n", t)
    return t


@task(container_image=image)
def print_table_by_second_dataclass(sd: MySecondDataClassDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MySecondDataClassDataset dataframe: \n", t)
    return t


@task(container_image=image)
def print_table_by_nested_dataclass(sd: MyNestedDataClassDataset) -> pd.DataFrame:
    t = sd.open(pd.DataFrame).all()
    print("MyNestedDataClassDataset dataframe: \n", t)
    return t


@workflow
def contacts_wf():
    sd = create_bq_table()
    print_table_by_arg(sd=sd)
    print_table_by_dict(sd=sd)
    print_table_by_list_dict(sd=sd)
    print_table_by_top_dataclass(sd=sd)
    print_table_by_second_dataclass(sd=sd)
    print_table_by_nested_dataclass(sd=sd)
    return


## Case 2.
@dataclass
class Levels:
    # level1: str
    level2: str


Schema = Annotated[StructuredDataset, kwtypes(age=int, levels=Levels)]


@task(container_image=image)
def mytask_w() -> StructuredDataset:
    df = pd.DataFrame({"age": [1, 2], "levels": [{"level1": "1", "level2": "2"}, {"level1": "2", "level2": "4"}]})
    return StructuredDataset(dataframe=df, uri=None)


# Should only show `level2` string.
@task(container_image=image)
def mytask_r(sd: Schema) -> list[str]:
    t = sd.open(pd.DataFrame).all()
    print("dataframe: \n", t)
    return t.columns.tolist()


@workflow
def levels_wf():
    sd = mytask_w()
    mytask_r(sd=sd)
    return

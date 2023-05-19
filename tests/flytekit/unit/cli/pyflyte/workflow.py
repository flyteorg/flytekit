import datetime
import enum
import typing
from dataclasses import dataclass

import pandas as pd
from dataclasses_json import dataclass_json
from typing_extensions import Annotated

from flytekit import kwtypes, task, workflow
from flytekit.types.file import FlyteFile
from flytekit.types.structured.structured_dataset import StructuredDataset

superset_cols = kwtypes(name=str, age=int)
subset_cols = kwtypes(age=int)


@task
def get_subset_df(df: Annotated[pd.DataFrame, superset_cols]) -> Annotated[StructuredDataset, subset_cols]:
    df = pd.concat([df, pd.DataFrame([[30]], columns=["age"])])
    return StructuredDataset(dataframe=df)


@task
def show_sd(in_sd: StructuredDataset):
    pd.set_option("expand_frame_repr", False)
    df = in_sd.open(pd.DataFrame).all()
    print(df)


@dataclass_json
@dataclass
class MyDataclass(object):
    i: int
    a: typing.List[str]


class Color(enum.Enum):
    RED = "RED"
    GREEN = "GREEN"
    BLUE = "BLUE"


@task
def print_all(
    a: int,
    b: str,
    c: float,
    d: MyDataclass,
    e: typing.List[int],
    f: typing.Dict[str, float],
    g: FlyteFile,
    h: bool,
    i: datetime.datetime,
    j: datetime.timedelta,
    k: Color,
    l: dict,
    m: dict,
    n: typing.List[typing.Dict[str, FlyteFile]],
    o: typing.Dict[str, typing.List[FlyteFile]],
    p: typing.Any,
):
    print(f"{a}, {b}, {c}, {d}, {e}, {f}, {g}, {h}, {i}, {j}, {k}, {l}, {m}, {n}, {o} , {p}")


@task
def test_union1(a: typing.Union[int, FlyteFile, typing.Dict[str, float], datetime.datetime, Color]):
    print(a)


@task
def test_union2(a: typing.Union[float, typing.List[int], MyDataclass]):
    print(a)


@workflow
def my_wf(
    a: int,
    b: str,
    c: float,
    d: MyDataclass,
    e: typing.List[int],
    f: typing.Dict[str, float],
    g: FlyteFile,
    h: bool,
    i: datetime.datetime,
    j: datetime.timedelta,
    k: Color,
    l: dict,
    n: typing.List[typing.Dict[str, FlyteFile]],
    o: typing.Dict[str, typing.List[FlyteFile]],
    p: typing.Any,
    remote: pd.DataFrame,
    image: StructuredDataset,
    m: dict = {"hello": "world"},
) -> Annotated[StructuredDataset, subset_cols]:
    x = get_subset_df(df=remote)  # noqa: shown for demonstration; users should use the same types between tasks
    show_sd(in_sd=x)
    show_sd(in_sd=image)
    print_all(a=a, b=b, c=c, d=d, e=e, f=f, g=g, h=h, i=i, j=j, k=k, l=l, m=m, n=n, o=o, p=p)
    return x

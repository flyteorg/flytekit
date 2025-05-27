from dataclasses import dataclass
from typing import Optional

from flytekit import task, workflow


@dataclass
class MyDataClassWithOptional:
    foo: dict[str, float]
    bar: Optional[dict] = None
    baz: Optional[dict[str, float]] = None
    qux: Optional[dict[str, int]] = None


@task
def t1(in_dataclass: MyDataClassWithOptional) -> MyDataClassWithOptional:
    return in_dataclass


@workflow
def wf(in_dataclass: MyDataClassWithOptional) -> MyDataClassWithOptional:
    return t1(in_dataclass=in_dataclass)  # type: ignore


@dataclass
class MyParentDataClass:
    child: MyDataClassWithOptional
    a: Optional[dict[str, float]] = None
    b: Optional[MyDataClassWithOptional] = None


@task
def t2(in_dataclass: MyParentDataClass) -> MyParentDataClass:
    return in_dataclass


@workflow
def wf_nested_dc(in_dataclass: MyParentDataClass) -> MyParentDataClass:
    return t2(in_dataclass=in_dataclass)  # type: ignore
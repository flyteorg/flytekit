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

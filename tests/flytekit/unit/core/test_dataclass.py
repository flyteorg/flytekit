import enum
import typing
from dataclasses import dataclass
from typing import Annotated, Dict, List

import pandas as pd
from dataclasses_json import dataclass_json

from flytekit import StructuredDataset, current_context, kwtypes
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.types.directory import FlyteDirectory
from flytekit.types.structured.structured_dataset import PARQUET


def test_dataclass():
    @dataclass_json
    @dataclass
    class AppParams(object):
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    @task
    def t1() -> AppParams:
        ap = AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"])
        return ap

    @workflow
    def wf() -> AppParams:
        return t1()

    res = wf()
    assert res.region == "us-west-3"


def test_dataclass_no_json():
    @dataclass
    class AppParams(object):
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    @task
    def t1() -> AppParams:
        ap = AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"])
        return ap

    @workflow
    def wf() -> AppParams:
        return t1()

    res = wf()
    assert res.region == "us-west-3"


def test_dataclass_complex_types():
    class AnEnum(enum.Enum):
        ONE = "one"
        TWO = "two"

    @dataclass
    class AppParams:
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    @dataclass
    class ComplexTypes:
        app_params: AppParams
        enum: AnEnum
        dir: FlyteDirectory
        dataset: Annotated[StructuredDataset, kwtypes(column=str), PARQUET]
        pickle_data: Foo

    @task
    def t1() -> ComplexTypes:
        ap = ComplexTypes(
            app_params=AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
            enum=AnEnum.ONE,
            dir=FlyteDirectory(path=current_context().working_directory),
            dataset=StructuredDataset(dataframe=pd.DataFrame(dict(column=["foo", "bar"]))),
            pickle_data=Foo(1),
        )
        return ap

    @workflow
    def wf() -> ComplexTypes:
        return t1()

    res = wf()
    assert res.app_params.region == "us-west-3"


def test_dataclass_dict():
    @dataclass
    class AppParams(object):
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    @task
    def first(params: Dict[str, AppParams]) -> AppParams:
        return params["first"]

    @workflow
    def wf(params: Dict[str, AppParams]) -> AppParams:
        return first(params=params)

    res = wf(
        params={
            "first": AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
            "later": AppParams(snapshotDate="4/6/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
        }
    )
    assert res.snapshotDate == "4/5/2063"


def test_generalization():
    @dataclass_json
    @dataclass
    class MyContainer(object):
        snapshot_date_str: bool
        list_keys: List[str]

    @task
    def get_true() -> bool:
        return True

    @task
    def get_strs() -> List[str]:
        return ["hello", "world"]

    @task
    def consume_and_print(a: MyContainer):
        print(a)

    @workflow
    def wf() -> MyContainer:
        b = get_true()
        l = get_strs()
        x = consume_and_print(a=MyContainer(snapshot_date_str=b, list_keys=l))
        other_task(b=x.snapshot_date_str)
        return MyContainer(snapshot_date_str=b, list_keys=l)

    @workflow
    def example_for_map() -> typing.Dict[str, List[str]]:
        a = get_strs()
        b = get_strs()
        consume_and_print(a={"first_strings": a, "second_strings": b})
        return {"first_strings": a, "second_strings": b}


# should general dataclasses be a scalar or a map?
# definitely a map, because we need to be able to bind multiple promises.
# feels very weird to have a scalar literal being fed by a bindingdata map

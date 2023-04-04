import enum
from dataclasses import dataclass
from typing import List, Dict, Annotated

import pandas as pd
from dataclasses_json import dataclass_json

from flytekit import kwtypes, StructuredDataset, current_context
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
        ONE = 'one'
        TWO = 'two'

    @dataclass
    class AppParams:
        snapshotDate: str
        region: str
        preprocess: bool
        listKeys: List[str]

    @dataclass
    class AppParamAndEnum:
        app_params: AppParams
        enum: AnEnum
        dir: FlyteDirectory
        dataset: Annotated[StructuredDataset, kwtypes(column=str), PARQUET]

    @task
    def t1() -> AppParamAndEnum:
        ap = AppParamAndEnum(
            app_params=AppParams(snapshotDate="4/5/2063",
                                 region="us-west-3",
                                 preprocess=False,
                                 listKeys=["a", "b"]),
            enum=AnEnum.ONE,
            dir=FlyteDirectory(path=current_context().working_directory),
            dataset=StructuredDataset(dataframe=pd.DataFrame(dict(
                column=['foo', 'bar'])))
        )
        return ap

    @workflow
    def wf() -> AppParamAndEnum:
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
        return params['first']
    @workflow
    def wf(params: Dict[str, AppParams]) -> AppParams:
        return first(params=params)

    res = wf(params={
        'first': AppParams(snapshotDate="4/5/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"]),
        'later': AppParams(snapshotDate="4/6/2063", region="us-west-3", preprocess=False, listKeys=["a", "b"])
    })
    assert res.snapshotDate == "4/5/2063"

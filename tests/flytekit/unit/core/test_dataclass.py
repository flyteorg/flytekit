import sys
from dataclasses import dataclass
from typing import List

import pytest
from dataclasses_json import DataClassJsonMixin
from mashumaro.mixins.json import DataClassJSONMixin
from typing_extensions import Annotated

from flytekit.core.task import task
from flytekit.core.type_engine import DataclassTransformer
from flytekit.core.workflow import workflow


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
def test_dataclass():
    @dataclass
    class AppParams(DataClassJsonMixin):
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


def test_dataclass_assert_works_for_annotated():
    @dataclass
    class MyDC(DataClassJSONMixin):
        my_str: str

    d = Annotated[MyDC, "tag"]
    DataclassTransformer().assert_type(d, MyDC(my_str="hi"))

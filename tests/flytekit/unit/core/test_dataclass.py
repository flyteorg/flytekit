from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from flytekit.core.task import task
from flytekit.core.workflow import workflow


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

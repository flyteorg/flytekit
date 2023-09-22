from dataclasses import dataclass
from typing import Dict, List, NamedTuple

from dataclasses_json import dataclass_json

from flytekit import WorkflowFailurePolicy, task, workflow


@dataclass_json
@dataclass
class foo:
    a: str


@task
def t1() -> (List[str], Dict[str, str], foo):
    return ["a", "b"], {"a": "b"}, foo(a="b")


@task
def t2(a: str) -> str:
    print("a", a)
    return a


@task
def t3() -> (Dict[str, List[str]], List[Dict[str, str]], Dict[str, foo], List[List[str]]):
    return {"a": ["b"]}, [{"a": "b"}], {"a": foo(a="b")}, [["a", "b"]]


@task
def t4(a: List[str]):
    print("a", a)


@task
def t5(a: Dict[str, str]):
    print("a", a)


@task
def t6(a: List[List[str]]):
    print(a)


@workflow
def basic_workflow():
    l, d, f = t1()
    t2(a=l[0])
    t2(a=d["a"])
    t2(a=f.a)


@workflow(
    # The workflow doesn't fail when one of the nodes fails but other nodes are still executable
    failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE
)
def failed_workflow():
    # This workflow is supposed to fail due to exceptions
    l, d, f = t1()
    t2(a=l[100])
    t2(a=d["b"])
    t2(a=f.b)


@workflow
def advanced_workflow():
    dl, ld, dd, ll = t3()
    t2(a=dl["a"][0])
    t2(a=ld[0]["a"])
    t2(a=dd["a"].a)

    # t4(a=dl["a"])
    # t5(a=ld[0])
    # t6(a=ll)

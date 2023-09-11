from flytekit import task, workflow
from typing import List, Dict, NamedTuple
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class foo:
    a: str
    b: List[str]
    c: Dict[str, str]

bar = NamedTuple("bar", a=str) # If want to use namedtuple as output, you can only have one output which is the namedtuple

@task
def t1() -> (List[str], Dict[str, str], foo, str):
    return ["a", "b"], {"a": "b"}, foo(a="a", b=["b1", "b2"], c={"c1": "c2"}), "a"

@task
def t2(a: str):
    print("a", a)
    # import pdb; pdb.set_trace()
    return a

@task
def t3(a: List[str]):
    return

@task
def t4(a: Dict[str, str]):
    return

@workflow
def my_workflow():
    l, d, f, a = t1()
    # i = t3()
    # t2(a= i)
    # t2(a=d["a"][0]) # working locally!
    # import pdb; pdb.set_trace()
    # t2(a=l[0]) # working locally!
    # t2(a=d["a"])
    t2(a=f.a)
    t3(a=f.b)
    t4(a=f.c)
    t4(a=f.c.b)
    # t2(a=d["a"][0].c["c1"])
    # t2(a=d["a"][0].b[0])
    # import pdb; pdb.set_trace()
    # t2(a=f.a)
    # t2(b.a)
    

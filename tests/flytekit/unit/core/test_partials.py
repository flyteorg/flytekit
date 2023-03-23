from collections import OrderedDict
from dataclasses import dataclass
import typing
import pytest
import pandas as pd
from dataclasses_json import dataclass_json
from flytekit.core.task import TaskMetadata
import flytekit.configuration
import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.map_task import map_task
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.tools.translator import get_serializable
from flytekit.core.dynamic_workflow_task import dynamic
from functools import partial
from flytekit.core.map_task import MapTaskResolver

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@dataclass_json
@dataclass
class MyParams(object):
    region: str


df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})


def test_basics_1():
    @task
    def t1(a: int, b: str, c: float) -> int:
        return a + len(b) + int(c)

    outside_p = partial(t1, b="hello", c=3.14)

    @workflow
    def my_wf_1(a: int) -> typing.Tuple[int, int]:
        inner_partial = partial(t1, b="world", c=2.7)
        out = outside_p(a=a)
        inside = inner_partial(a=a)
        return out, inside

    with pytest.raises(Exception):
        get_serializable(OrderedDict(), serialization_settings, outside_p)

    # check the od todo
    wf_1_spec = get_serializable(OrderedDict(), serialization_settings, my_wf_1)
    print(wf_1_spec)
    # check only one task in the spec

    @task
    def get_str() -> str:
        return "got str"

    bind_c = partial(t1, c=2.7)

    @workflow
    def my_wf_2(a: int) -> int:
        s = get_str()
        inner_partial = partial(bind_c, b=s)
        inside = inner_partial(a=a)
        return inside

    # check the od todo
    wf_2_spec = get_serializable(OrderedDict(), serialization_settings, my_wf_2)
    print(wf_2_spec)
    # check only one task in the spec


def test_map_task_types():
    @task(cache=True, cache_version="1")
    def t3(a: int, b: str, c: float) -> str:
        return str(a) + b + str(c)

    t3_bind_b1 = partial(t3, b="hello")
    t3_bind_b2 = partial(t3, b="world")
    t3_bind_c1 = partial(t3_bind_b1, c=3.14)
    t3_bind_c2 = partial(t3_bind_b2, c=2.78)

    mt1 = map_task(t3_bind_c1, metadata=TaskMetadata(cache=True, cache_version="1"))
    mt2 = map_task(t3_bind_c2, metadata=TaskMetadata(cache=True, cache_version="1"))

    @task
    def print_lists(i: typing.List[str], j: typing.List[str]):
        print(f"First: {i}")
        print(f"Second: {j}")

    @workflow
    def wf_out(a: typing.List[int]):
        i = mt1(a=a)
        j = mt2(a=[3, 4, 5])
        print_lists(i=i, j=j)

    wf_out(a=[1, 2])

    @workflow
    def wf_in(a: typing.List[int]):
        mt_in1 = map_task(t3_bind_c1, metadata=TaskMetadata(cache=True, cache_version="1"))
        mt_in2 = map_task(t3_bind_c2, metadata=TaskMetadata(cache=True, cache_version="1"))
        i = mt_in1(a=a)
        j = mt_in2(a=[3, 4, 5])
        print_lists(i=i, j=j)

    wf_in(a=[1, 2])


def test_everything():
    @task
    def get_static_list() -> typing.List[float]:
        return [3.14, 2.718]

    @task
    def t3(a: int, b: str, c: typing.List[float], d: typing.List[float]) -> str:
        return str(a) + b + str(c) + "&&" + str(d)

    t3_bind_b1 = partial(t3, b="hello")
    t3_bind_b2 = partial(t3, b="world")
    t3_bind_c1 = partial(t3_bind_b1, c=[6.674, 1.618, 6.626], d=[1.])

    mt1 = map_task(t3_bind_c1)

    mr = MapTaskResolver()
    aa = mr.loader_args(serialization_settings, mt1)
    assert "b,c,d" in str(aa)

    @task
    def print_lists(i: typing.List[str], j: typing.List[str]) -> str:
        print(f"First: {i}")
        print(f"Second: {j}")
        return f"{i}-{j}"

    @dynamic
    def dt1(a: typing.List[int], sl: typing.List[float]) -> str:
        i = mt1(a=a)
        t3_bind_c2 = partial(t3_bind_b2, c=[1., 2., 3.], d=sl)
        mt_in2 = map_task(t3_bind_c2)
        j = mt_in2(a=[3, 4, 5])
        return print_lists(i=i, j=j)

    @workflow
    def wf_dt(a: typing.List[int]) -> str:
        sl = get_static_list()
        return dt1(a=a, sl=sl)

    assert wf_dt(a=[1, 2]) == "['1hello[6.674, 1.618, 6.626]&&[1.0]', '2hello[6.674, 1.618, 6.626]&&[1.0]']-['3world[1.0, 2.0, 3.0]&&[3.14, 2.718]', '4world[1.0, 2.0, 3.0]&&[3.14, 2.718]', '5world[1.0, 2.0, 3.0]&&[3.14, 2.718]']"

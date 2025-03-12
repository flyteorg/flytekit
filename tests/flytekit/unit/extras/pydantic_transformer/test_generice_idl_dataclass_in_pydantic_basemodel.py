from pydantic import BaseModel, Field
import pytest
import os
import copy

from flytekit import task, workflow

@pytest.fixture(autouse=True)
def prepare_env_variable():
    try:
        origin_env = copy.deepcopy(os.environ.copy())
        os.environ["FLYTE_USE_OLD_DC_FORMAT"] = "True"
        yield
    finally:
        os.environ = origin_env


def test_dataclasss_in_pydantic_basemodel():
    from dataclasses import dataclass

    @dataclass
    class InnerBM:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    class BM(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())

    @task
    def t_bm(bm: BM):
        assert isinstance(bm, BM)
        assert isinstance(bm.inner_bm, InnerBM)

    @task
    def t_inner(inner_bm: InnerBM):
        assert isinstance(inner_bm, InnerBM)

    @task
    def t_test_primitive_attributes(a: int, b: float, c: str, d: bool):
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert b == 3.14
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert c == "Hello, Flyte"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"
        assert d is False
        print("All primitive attributes passed strict type checks.")

    @workflow
    def wf(bm: BM):
        t_bm(bm=bm)
        t_inner(inner_bm=bm.inner_bm)
        t_test_primitive_attributes(a=bm.a, b=bm.b, c=bm.c, d=bm.d)
        t_test_primitive_attributes(
            a=bm.inner_bm.a, b=bm.inner_bm.b, c=bm.inner_bm.c, d=bm.inner_bm.d
        )

    bm = BM()
    wf(bm=bm)


def test_pydantic_dataclasss_in_pydantic_basemodel():
    from pydantic.dataclasses import dataclass

    @dataclass
    class InnerBM:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    class BM(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())

    @task
    def t_bm(bm: BM):
        assert isinstance(bm, BM)
        assert isinstance(bm.inner_bm, InnerBM)

    @task
    def t_inner(inner_bm: InnerBM):
        assert isinstance(inner_bm, InnerBM)

    @task
    def t_test_primitive_attributes(a: int, b: float, c: str, d: bool):
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert b == 3.14
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert c == "Hello, Flyte"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"
        assert d is False
        print("All primitive attributes passed strict type checks.")

    @workflow
    def wf(bm: BM):
        t_bm(bm=bm)
        t_inner(inner_bm=bm.inner_bm)
        t_test_primitive_attributes(a=bm.a, b=bm.b, c=bm.c, d=bm.d)
        t_test_primitive_attributes(
            a=bm.inner_bm.a, b=bm.inner_bm.b, c=bm.inner_bm.c, d=bm.inner_bm.d
        )

    bm = BM()
    wf(bm=bm)

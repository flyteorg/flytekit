import pytest
from pydantic import BaseModel

from flytekit import task, workflow

"""
This should work in the future, either datclass in pydantic basemodel or pydantic dataclass in pydantic basemodel.
"""


def test_pydantic_basemodel_in_dataclass():
    from dataclasses import dataclass, field

    class InnerBM(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    @dataclass
    class DC:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_bm: InnerBM = field(default_factory=lambda: InnerBM())

    @task
    def t_dc(dc: DC):
        assert isinstance(dc, DC)
        assert isinstance(dc.inner_bm, InnerBM)

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
    def wf(dc: DC):
        t_dc(dc=dc)
        t_inner(inner_bm=dc.inner_bm)
        t_test_primitive_attributes(a=dc.a, b=dc.b, c=dc.c, d=dc.d)
        t_test_primitive_attributes(
            a=dc.inner_bm.a,
            b=dc.inner_bm.b,
            c=dc.inner_bm.c,
            d=dc.inner_bm.d)

    dc_instance = DC()
    with pytest.raises(Exception) as excinfo:
        wf(dc=dc_instance)
        assert "UnserializableField" in str(
            excinfo.value), f"Unexpected error: {
            excinfo.value}"


def test_pydantic_basemodel_in_pydantic_dataclass():
    from pydantic import Field
    from pydantic.dataclasses import dataclass

    class InnerBM(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    @dataclass
    class DC:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())

    @task
    def t_dc(dc: DC):
        assert isinstance(dc, DC)
        assert isinstance(dc.inner_bm, InnerBM)

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
    def wf(dc: DC):
        t_dc(dc=dc)
        t_inner(inner_bm=dc.inner_bm)
        t_test_primitive_attributes(a=dc.a, b=dc.b, c=dc.c, d=dc.d)
        t_test_primitive_attributes(
            a=dc.inner_bm.a,
            b=dc.inner_bm.b,
            c=dc.inner_bm.c,
            d=dc.inner_bm.d)

    dc_instance = DC()
    with pytest.raises(Exception) as excinfo:
        wf(dc=dc_instance)
        assert "UnserializableField" in str(
            excinfo.value), f"Unexpected error: {
            excinfo.value}"

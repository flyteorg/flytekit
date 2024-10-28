import pytest
from pydantic import BaseModel

from flytekit import task, workflow

"""
This should be supported in the future.
Issue Link: https://github.com/flyteorg/flyte/issues/5925
"""


def test_pydantic_basemodel_in_dataclass():
    from dataclasses import dataclass, field

    # Define InnerBM using Pydantic BaseModel
    class InnerBM(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    # Define the dataclass DC
    @dataclass
    class DC:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_bm: InnerBM = field(default_factory=lambda: InnerBM())

    # Task to check DC instance
    @task
    def t_dc(dc: DC):
        assert isinstance(dc, DC)
        assert isinstance(dc.inner_bm, InnerBM)

    # Task to check InnerBM instance
    @task
    def t_inner(inner_bm: InnerBM):
        assert isinstance(inner_bm, InnerBM)

    # Task to check primitive attributes
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

    # Define the workflow
    @workflow
    def wf(dc: DC):
        t_dc(dc=dc)
        t_inner(inner_bm=dc.inner_bm)
        t_test_primitive_attributes(a=dc.a, b=dc.b, c=dc.c, d=dc.d)
        t_test_primitive_attributes(
            a=dc.inner_bm.a, b=dc.inner_bm.b, c=dc.inner_bm.c, d=dc.inner_bm.d
        )

    # Create an instance of DC and run the workflow
    dc_instance = DC()
    with pytest.raises(Exception) as excinfo:
        wf(dc=dc_instance)

    # Assert that the error message contains "UnserializableField"
    assert "is not serializable" in str(
        excinfo.value
    ), f"Unexpected error: {excinfo.value}"


def test_pydantic_basemodel_in_pydantic_dataclass():
    from pydantic import Field
    from pydantic.dataclasses import dataclass

    # Define InnerBM using Pydantic BaseModel
    class InnerBM(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    # Define the Pydantic dataclass DC
    @dataclass
    class DC:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())

    # Task to check DC instance
    @task
    def t_dc(dc: DC):
        assert isinstance(dc, DC)
        assert isinstance(dc.inner_bm, InnerBM)

    # Task to check InnerBM instance
    @task
    def t_inner(inner_bm: InnerBM):
        assert isinstance(inner_bm, InnerBM)

    # Task to check primitive attributes
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

    # Define the workflow
    @workflow
    def wf(dc: DC):
        t_dc(dc=dc)
        t_inner(inner_bm=dc.inner_bm)
        t_test_primitive_attributes(a=dc.a, b=dc.b, c=dc.c, d=dc.d)
        t_test_primitive_attributes(
            a=dc.inner_bm.a, b=dc.inner_bm.b, c=dc.inner_bm.c, d=dc.inner_bm.d
        )

    # Create an instance of DC and run the workflow
    dc_instance = DC()
    with pytest.raises(Exception) as excinfo:
        wf(dc=dc_instance)

    # Assert that the error message contains "UnserializableField"
    assert "is not serializable" in str(
        excinfo.value
    ), f"Unexpected error: {excinfo.value}"

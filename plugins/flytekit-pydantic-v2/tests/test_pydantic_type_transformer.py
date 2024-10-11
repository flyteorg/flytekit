import os
import tempfile
from dataclasses import field
from enum import Enum
from typing import Dict, List
from pydantic import BaseModel, Field

import pytest
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit import task, workflow
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.models.literals import Literal, Scalar
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

class Status(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


@pytest.fixture
def local_dummy_file():
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.write("Hello FlyteFile")
        yield path
    finally:
        os.remove(path)


@pytest.fixture
def local_dummy_directory():
    temp_dir = tempfile.TemporaryDirectory()
    try:
        with open(os.path.join(temp_dir.name, "file"), "w") as tmp:
            tmp.write("Hello FlyteDirectory")
        yield temp_dir.name
    finally:
        temp_dir.cleanup()


def test_flytetypes_in_pydantic_basemodel_wf(local_dummy_file, local_dummy_directory):
    class InnerDC(BaseModel):
        flytefile: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        flytedir: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))

    class DC(BaseModel):
        flytefile: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        flytedir: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())

    @task
    def t1(path: FlyteFile) -> FlyteFile:
        return path

    @task
    def t2(path: FlyteDirectory) -> FlyteDirectory:
        return path

    @workflow
    def wf(dc: DC) -> (FlyteFile, FlyteFile, FlyteDirectory, FlyteDirectory):
        f1 = t1(path=dc.flytefile)
        f2 = t1(path=dc.inner_dc.flytefile)
        d1 = t2(path=dc.flytedir)
        d2 = t2(path=dc.inner_dc.flytedir)
        return f1, f2, d1, d2

    o1, o2, o3, o4 = wf(dc=DC())
    with open(o1, "r") as fh:
        assert fh.read() == "Hello FlyteFile"

    with open(o2, "r") as fh:
        assert fh.read() == "Hello FlyteFile"

    with open(os.path.join(o3, "file"), "r") as fh:
        assert fh.read() == "Hello FlyteDirectory"

    with open(os.path.join(o4, "file"), "r") as fh:
        assert fh.read() == "Hello FlyteDirectory"

def test_all_types_in_pydantic_basemodel_wf(local_dummy_file, local_dummy_directory):
    class InnerDC(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile(local_dummy_file)])
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
        i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
        j: Dict[int, FlyteFile] = field(default_factory=lambda: {0: FlyteFile(local_dummy_file),
                                                                 1: FlyteFile(local_dummy_file),
                                                                 -1: FlyteFile(local_dummy_file)})
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))
        enum_status: Status = field(default=Status.PENDING)

    class DC(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile(local_dummy_file), ])
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
        i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
        j: Dict[int, FlyteFile] = field(default_factory=lambda: {0: FlyteFile(local_dummy_file),
                                                                 1: FlyteFile(local_dummy_file),
                                                                 -1: FlyteFile(local_dummy_file)})
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
        enum_status: Status = field(default=Status.PENDING)

    @task
    def t_inner(inner_dc: InnerDC):
        assert (type(inner_dc), InnerDC) # type: ignore

        # f: List[FlyteFile]
        for ff in inner_dc.f:
            assert (type(ff), FlyteFile) # type: ignore
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_dc.j.items():
            assert (type(ff), FlyteFile) # type: ignore
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert (type(inner_dc.n), FlyteFile) # type: ignore
        with open(inner_dc.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert (type(inner_dc.o), FlyteDirectory) # type: ignore
        assert not inner_dc.o.downloaded
        with open(os.path.join(inner_dc.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_dc.o.downloaded
        # enum: Status
        assert inner_dc.enum_status == Status.PENDING


    @task
    def t_test_all_attributes(a: int, b: float, c: str, d: bool, e: List[int], f: List[FlyteFile], g: List[List[int]],
                              h: List[Dict[int, bool]], i: Dict[int, bool], j: Dict[int, FlyteFile],
                              k: Dict[int, List[int]], l: Dict[int, Dict[int, int]], m: dict,
                              n: FlyteFile, o: FlyteDirectory, enum_status: Status):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(isinstance(i, int) for i in e), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(isinstance(i, FlyteFile) for i in f), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict) and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items()) for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int) and isinstance(v, list) and all(isinstance(i, int) for i in v) for k, v in
            k.items()), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int) and isinstance(v, dict) and all(
                isinstance(sub_k, int) and isinstance(sub_v, int) for sub_k, sub_v in v.items())
            for k, v in l.items()), "l is not Dict[int, Dict[int, int]]"

        # Strict type check for a generic dict
        assert isinstance(m, dict), "m is not dict"

        # Strict type check for FlyteFile
        assert isinstance(n, FlyteFile), "n is not FlyteFile"

        # Strict type check for FlyteDirectory
        assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

        # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

        print("All attributes passed strict type checks.")

    @workflow
    def wf(dc: DC):
        t_inner(dc.inner_dc)
        t_test_all_attributes(a=dc.a, b=dc.b, c=dc.c,
                              d=dc.d, e=dc.e, f=dc.f,
                              g=dc.g, h=dc.h, i=dc.i,
                              j=dc.j, k=dc.k, l=dc.l,
                              m=dc.m, n=dc.n, o=dc.o, enum_status=dc.enum_status)

        t_test_all_attributes(a=dc.inner_dc.a, b=dc.inner_dc.b, c=dc.inner_dc.c,
                              d=dc.inner_dc.d, e=dc.inner_dc.e, f=dc.inner_dc.f,
                              g=dc.inner_dc.g, h=dc.inner_dc.h, i=dc.inner_dc.i,
                              j=dc.inner_dc.j, k=dc.inner_dc.k, l=dc.inner_dc.l,
                              m=dc.inner_dc.m, n=dc.inner_dc.n, o=dc.inner_dc.o, enum_status=dc.inner_dc.enum_status)

    wf(dc=DC())

def test_input_from_flyte_console_pydantic_basemodel(local_dummy_file, local_dummy_directory):
    # Flyte Console will send the input data as protobuf Struct

    class InnerDC(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile(local_dummy_file)])
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
        i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
        j: Dict[int, FlyteFile] = field(default_factory=lambda: {0: FlyteFile(local_dummy_file),
                                                                 1: FlyteFile(local_dummy_file),
                                                                 -1: FlyteFile(local_dummy_file)})
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))
        enum_status: Status = field(default=Status.PENDING)

    class DC(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile(local_dummy_file), ])
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
        i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
        j: Dict[int, FlyteFile] = field(default_factory=lambda: {0: FlyteFile(local_dummy_file),
                                                                 1: FlyteFile(local_dummy_file),
                                                                 -1: FlyteFile(local_dummy_file)})
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
        enum_status: Status = field(default=Status.PENDING)

    def t_inner(inner_dc: InnerDC):
        assert (type(inner_dc), InnerDC) # type: ignore

        # f: List[FlyteFile]
        for ff in inner_dc.f:
            assert (type(ff), FlyteFile) # type: ignore
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_dc.j.items():
            assert (type(ff), FlyteFile) # type: ignore
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert (type(inner_dc.n), FlyteFile) # type: ignore
        with open(inner_dc.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert (type(inner_dc.o), FlyteDirectory) # type: ignore
        assert not inner_dc.o.downloaded
        with open(os.path.join(inner_dc.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_dc.o.downloaded
        print("Test InnerDC Successfully Passed")
        # enum: Status
        assert inner_dc.enum_status == Status.PENDING

    def t_test_all_attributes(a: int, b: float, c: str, d: bool, e: List[int], f: List[FlyteFile], g: List[List[int]],
                              h: List[Dict[int, bool]], i: Dict[int, bool], j: Dict[int, FlyteFile],
                              k: Dict[int, List[int]], l: Dict[int, Dict[int, int]], m: dict,
                              n: FlyteFile, o: FlyteDirectory, enum_status: Status):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(isinstance(i, int) for i in e), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(isinstance(i, FlyteFile) for i in f), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict) and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items()) for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int) and isinstance(v, list) and all(isinstance(i, int) for i in v) for k, v in
            k.items()), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int) and isinstance(v, dict) and all(
                isinstance(sub_k, int) and isinstance(sub_v, int) for sub_k, sub_v in v.items())
            for k, v in l.items()), "l is not Dict[int, Dict[int, int]]"

        # Strict type check for a generic dict
        assert isinstance(m, dict), "m is not dict"

        # Strict type check for FlyteFile
        assert isinstance(n, FlyteFile), "n is not FlyteFile"

        # Strict type check for FlyteDirectory
        assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

        # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

        print("All attributes passed strict type checks.")

    # This is the old dataclass serialization behavior.
    # https://github.com/flyteorg/flytekit/blob/94786cfd4a5c2c3b23ac29dcd6f04d0553fa1beb/flytekit/core/type_engine.py#L702-L728
    dc = DC()
    json_str = dc.model_dump_json()
    upstream_output = Literal(scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())))

    downstream_input = TypeEngine.to_python_value(FlyteContextManager.current_context(), upstream_output, DC)
    t_inner(downstream_input.inner_dc)
    t_test_all_attributes(a=downstream_input.a, b=downstream_input.b, c=downstream_input.c,
                          d=downstream_input.d, e=downstream_input.e, f=downstream_input.f,
                          g=downstream_input.g, h=downstream_input.h, i=downstream_input.i,
                          j=downstream_input.j, k=downstream_input.k, l=downstream_input.l,
                          m=downstream_input.m, n=downstream_input.n, o=downstream_input.o,
                          enum_status=downstream_input.enum_status)
    t_test_all_attributes(a=downstream_input.inner_dc.a, b=downstream_input.inner_dc.b, c=downstream_input.inner_dc.c,
                          d=downstream_input.inner_dc.d, e=downstream_input.inner_dc.e, f=downstream_input.inner_dc.f,
                          g=downstream_input.inner_dc.g, h=downstream_input.inner_dc.h, i=downstream_input.inner_dc.i,
                          j=downstream_input.inner_dc.j, k=downstream_input.inner_dc.k, l=downstream_input.inner_dc.l,
                          m=downstream_input.inner_dc.m, n=downstream_input.inner_dc.n, o=downstream_input.inner_dc.o,
                          enum_status=downstream_input.inner_dc.enum_status)

def test_dataclasss_in_pydantic_basemodel():
    from dataclasses import dataclass
    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    class DC(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_dc: InnerDC = Field(default_factory=lambda: InnerDC())

    @task
    def t_dc(dc: DC):
        assert isinstance(dc, DC)
        assert isinstance(dc.inner_dc, InnerDC)

    @task
    def t_inner(inner_dc: InnerDC):
        assert isinstance(inner_dc, InnerDC)

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
        t_inner(inner_dc=dc.inner_dc)
        t_test_primitive_attributes(a=dc.a, b=dc.b, c=dc.c, d=dc.d)
        t_test_primitive_attributes(a=dc.inner_dc.a, b=dc.inner_dc.b, c=dc.inner_dc.c, d=dc.inner_dc.d)

    dc = DC()
    wf(dc=dc)

def test_pydantic_dataclasss_in_pydantic_basemodel():
    from pydantic.dataclasses import dataclass
    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False

    class DC(BaseModel):
        a: int = -1
        b: float = 3.14
        c: str = "Hello, Flyte"
        d: bool = False
        inner_dc: InnerDC = Field(default_factory=lambda: InnerDC())

    @task
    def t_dc(dc: DC):
        assert isinstance(dc, DC)
        assert isinstance(dc.inner_dc, InnerDC)

    @task
    def t_inner(inner_dc: InnerDC):
        assert isinstance(inner_dc, InnerDC)

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
        t_inner(inner_dc=dc.inner_dc)
        t_test_primitive_attributes(a=dc.a, b=dc.b, c=dc.c, d=dc.d)
        t_test_primitive_attributes(a=dc.inner_dc.a, b=dc.inner_dc.b, c=dc.inner_dc.c, d=dc.inner_dc.d)

    dc = DC()
    wf(dc=dc)

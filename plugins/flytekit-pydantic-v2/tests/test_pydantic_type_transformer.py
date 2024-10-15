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
    class InnerBM(BaseModel):
        flytefile: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        flytedir: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))

    class BM(BaseModel):
        flytefile: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        flytedir: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_dummy_directory))
        inner_bm: InnerBM = field(default_factory=lambda: InnerBM())

    @task
    def t1(path: FlyteFile) -> FlyteFile:
        return path

    @task
    def t2(path: FlyteDirectory) -> FlyteDirectory:
        return path

    @workflow
    def wf(bm: BM) -> (FlyteFile, FlyteFile, FlyteDirectory, FlyteDirectory):
        f1 = t1(path=bm.flytefile)
        f2 = t1(path=bm.inner_bm.flytefile)
        d1 = t2(path=bm.flytedir)
        d2 = t2(path=bm.inner_bm.flytedir)
        return f1, f2, d1, d2

    o1, o2, o3, o4 = wf(bm=BM())
    with open(o1, "r") as fh:
        assert fh.read() == "Hello FlyteFile"

    with open(o2, "r") as fh:
        assert fh.read() == "Hello FlyteFile"

    with open(os.path.join(o3, "file"), "r") as fh:
        assert fh.read() == "Hello FlyteDirectory"

    with open(os.path.join(o4, "file"), "r") as fh:
        assert fh.read() == "Hello FlyteDirectory"

def test_all_types_in_pydantic_basemodel_wf(local_dummy_file, local_dummy_directory):
    class InnerBM(BaseModel):
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

    class BM(BaseModel):
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
        inner_bm: InnerBM = field(default_factory=lambda: InnerBM())
        enum_status: Status = field(default=Status.PENDING)

    @task
    def t_inner(inner_bm: InnerBM):
        assert type(inner_bm) is InnerBM

        # f: List[FlyteFile]
        for ff in inner_bm.f:
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_bm.j.items():
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert type(inner_bm.n) is FlyteFile
        with open(inner_bm.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert type(inner_bm.o) is FlyteDirectory
        assert not inner_bm.o.downloaded
        with open(os.path.join(inner_bm.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_bm.o.downloaded

        # enum: Status
        assert inner_bm.enum_status == Status.PENDING


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
    def wf(bm: BM):
        t_inner(bm.inner_bm)
        t_test_all_attributes(a=bm.a, b=bm.b, c=bm.c,
                              d=bm.d, e=bm.e, f=bm.f,
                              g=bm.g, h=bm.h, i=bm.i,
                              j=bm.j, k=bm.k, l=bm.l,
                              m=bm.m, n=bm.n, o=bm.o, enum_status=bm.enum_status)

        t_test_all_attributes(a=bm.inner_bm.a, b=bm.inner_bm.b, c=bm.inner_bm.c,
                              d=bm.inner_bm.d, e=bm.inner_bm.e, f=bm.inner_bm.f,
                              g=bm.inner_bm.g, h=bm.inner_bm.h, i=bm.inner_bm.i,
                              j=bm.inner_bm.j, k=bm.inner_bm.k, l=bm.inner_bm.l,
                              m=bm.inner_bm.m, n=bm.inner_bm.n, o=bm.inner_bm.o, enum_status=bm.inner_bm.enum_status)

    wf(bm=BM())

def test_input_from_flyte_console_pydantic_basemodel(local_dummy_file, local_dummy_directory):
    # Flyte Console will send the input data as protobuf Struct

    class InnerBM(BaseModel):
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

    class BM(BaseModel):
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
        inner_bm: InnerBM = field(default_factory=lambda: InnerBM())
        enum_status: Status = field(default=Status.PENDING)

    @task
    def t_inner(inner_bm: InnerBM):
        assert type(inner_bm) is InnerBM

        # f: List[FlyteFile]
        for ff in inner_bm.f:
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_bm.j.items():
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert type(inner_bm.n) is FlyteFile
        with open(inner_bm.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert type(inner_bm.o) is FlyteDirectory
        assert not inner_bm.o.downloaded
        with open(os.path.join(inner_bm.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_bm.o.downloaded

        # enum: Status
        assert inner_bm.enum_status == Status.PENDING

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
    # https://github.com/flyteorg/flytekit/blob/94786cfd4a5c2c3b23ac29bmd6f04d0553fa1beb/flytekit/core/type_engine.py#L702-L728
    bm = BM()
    json_str = bm.model_dump_json()
    upstream_output = Literal(scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())))

    downstream_input = TypeEngine.to_python_value(FlyteContextManager.current_context(), upstream_output, BM)
    t_inner(downstream_input.inner_bm)
    t_test_all_attributes(a=downstream_input.a, b=downstream_input.b, c=downstream_input.c,
                          d=downstream_input.d, e=downstream_input.e, f=downstream_input.f,
                          g=downstream_input.g, h=downstream_input.h, i=downstream_input.i,
                          j=downstream_input.j, k=downstream_input.k, l=downstream_input.l,
                          m=downstream_input.m, n=downstream_input.n, o=downstream_input.o,
                          enum_status=downstream_input.enum_status)
    t_test_all_attributes(a=downstream_input.inner_bm.a, b=downstream_input.inner_bm.b, c=downstream_input.inner_bm.c,
                          d=downstream_input.inner_bm.d, e=downstream_input.inner_bm.e, f=downstream_input.inner_bm.f,
                          g=downstream_input.inner_bm.g, h=downstream_input.inner_bm.h, i=downstream_input.inner_bm.i,
                          j=downstream_input.inner_bm.j, k=downstream_input.inner_bm.k, l=downstream_input.inner_bm.l,
                          m=downstream_input.inner_bm.m, n=downstream_input.inner_bm.n, o=downstream_input.inner_bm.o,
                          enum_status=downstream_input.inner_bm.enum_status)

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
        t_test_primitive_attributes(a=bm.inner_bm.a, b=bm.inner_bm.b, c=bm.inner_bm.c, d=bm.inner_bm.d)

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
        t_test_primitive_attributes(a=bm.inner_bm.a, b=bm.inner_bm.b, c=bm.inner_bm.c, d=bm.inner_bm.d)

    bm = BM()
    wf(bm=bm)

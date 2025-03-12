import os
import tempfile
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union
from unittest.mock import patch
import mock
import pytest
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from pydantic import BaseModel, Field

from flytekit import task, workflow
from flytekit.core.constants import CACHE_KEY_METADATA, MESSAGEPACK, SERIALIZATION_FORMAT
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.models.annotation import TypeAnnotation
from flytekit.models.literals import Literal, Scalar
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema
from flytekit.types.structured import StructuredDataset

pd = pytest.importorskip("pandas")


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
        flytefile: FlyteFile = Field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        flytedir: FlyteDirectory = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )

    class BM(BaseModel):
        flytefile: FlyteFile = Field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        flytedir: FlyteDirectory = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())

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
        e: List[int] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = Field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: List[List[int]] = Field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = Field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = Field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = Field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = Field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = Field(default_factory=lambda: {1: {-1: 0}})
        m: dict = Field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = Field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        enum_status: Status = Field(default=Status.PENDING)

    class BM(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = Field(
            default_factory=lambda: [
                FlyteFile(local_dummy_file),
            ]
        )
        g: List[List[int]] = Field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = Field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = Field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = Field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = Field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = Field(default_factory=lambda: {1: {-1: 0}})
        m: dict = Field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = Field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())
        enum_status: Status = Field(default=Status.PENDING)

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
    def t_test_all_attributes(
        a: int,
        b: float,
        c: str,
        d: bool,
        e: List[int],
        f: List[FlyteFile],
        g: List[List[int]],
        h: List[Dict[int, bool]],
        i: Dict[int, bool],
        j: Dict[int, FlyteFile],
        k: Dict[int, List[int]],
        l: Dict[int, Dict[int, int]],
        m: dict,
        n: FlyteFile,
        o: FlyteDirectory,
        enum_status: Status,
    ):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(
            isinstance(i, int) for i in e
        ), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(
            isinstance(i, FlyteFile) for i in f
        ), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g
        ), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict)
            and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items())
            for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()
        ), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()
        ), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int)
            and isinstance(v, list)
            and all(isinstance(i, int) for i in v)
            for k, v in k.items()
        ), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int)
            and isinstance(v, dict)
            and all(
                isinstance(sub_k, int) and isinstance(sub_v, int)
                for sub_k, sub_v in v.items()
            )
            for k, v in l.items()
        ), "l is not Dict[int, Dict[int, int]]"

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
        t_test_all_attributes(
            a=bm.a,
            b=bm.b,
            c=bm.c,
            d=bm.d,
            e=bm.e,
            f=bm.f,
            g=bm.g,
            h=bm.h,
            i=bm.i,
            j=bm.j,
            k=bm.k,
            l=bm.l,
            m=bm.m,
            n=bm.n,
            o=bm.o,
            enum_status=bm.enum_status,
        )

        t_test_all_attributes(
            a=bm.inner_bm.a,
            b=bm.inner_bm.b,
            c=bm.inner_bm.c,
            d=bm.inner_bm.d,
            e=bm.inner_bm.e,
            f=bm.inner_bm.f,
            g=bm.inner_bm.g,
            h=bm.inner_bm.h,
            i=bm.inner_bm.i,
            j=bm.inner_bm.j,
            k=bm.inner_bm.k,
            l=bm.inner_bm.l,
            m=bm.inner_bm.m,
            n=bm.inner_bm.n,
            o=bm.inner_bm.o,
            enum_status=bm.inner_bm.enum_status,
        )

    wf(bm=BM())


def test_all_types_with_optional_in_pydantic_basemodel_wf(
    local_dummy_file, local_dummy_directory
):
    class InnerBM(BaseModel):
        a: Optional[int] = -1
        b: Optional[float] = 2.1
        c: Optional[str] = "Hello, Flyte"
        d: Optional[bool] = False
        e: Optional[List[int]] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: Optional[List[FlyteFile]] = Field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: Optional[List[List[int]]] = Field(default_factory=lambda: [[0], [1], [-1]])
        h: Optional[List[Dict[int, bool]]] = Field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Optional[Dict[int, bool]] = Field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Optional[Dict[int, FlyteFile]] = Field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Optional[Dict[int, List[int]]] = Field(
            default_factory=lambda: {0: [0, 1, -1]}
        )
        l: Optional[Dict[int, Dict[int, int]]] = Field(
            default_factory=lambda: {1: {-1: 0}}
        )
        m: Optional[dict] = Field(default_factory=lambda: {"key": "value"})
        n: Optional[FlyteFile] = Field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        o: Optional[FlyteDirectory] = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        enum_status: Optional[Status] = Field(default=Status.PENDING)

    class BM(BaseModel):
        a: Optional[int] = -1
        b: Optional[float] = 2.1
        c: Optional[str] = "Hello, Flyte"
        d: Optional[bool] = False
        e: Optional[List[int]] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: Optional[List[FlyteFile]] = Field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: Optional[List[List[int]]] = Field(default_factory=lambda: [[0], [1], [-1]])
        h: Optional[List[Dict[int, bool]]] = Field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Optional[Dict[int, bool]] = Field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Optional[Dict[int, FlyteFile]] = Field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Optional[Dict[int, List[int]]] = Field(
            default_factory=lambda: {0: [0, 1, -1]}
        )
        l: Optional[Dict[int, Dict[int, int]]] = Field(
            default_factory=lambda: {1: {-1: 0}}
        )
        m: Optional[dict] = Field(default_factory=lambda: {"key": "value"})
        n: Optional[FlyteFile] = Field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        o: Optional[FlyteDirectory] = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_bm: Optional[InnerBM] = Field(default_factory=lambda: InnerBM())
        enum_status: Optional[Status] = Field(default=Status.PENDING)

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
    def t_test_all_attributes(
        a: Optional[int],
        b: Optional[float],
        c: Optional[str],
        d: Optional[bool],
        e: Optional[List[int]],
        f: Optional[List[FlyteFile]],
        g: Optional[List[List[int]]],
        h: Optional[List[Dict[int, bool]]],
        i: Optional[Dict[int, bool]],
        j: Optional[Dict[int, FlyteFile]],
        k: Optional[Dict[int, List[int]]],
        l: Optional[Dict[int, Dict[int, int]]],
        m: Optional[dict],
        n: Optional[FlyteFile],
        o: Optional[FlyteDirectory],
        enum_status: Optional[Status],
    ):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(
            isinstance(i, int) for i in e
        ), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(
            isinstance(i, FlyteFile) for i in f
        ), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g
        ), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict)
            and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items())
            for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()
        ), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()
        ), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int)
            and isinstance(v, list)
            and all(isinstance(i, int) for i in v)
            for k, v in k.items()
        ), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int)
            and isinstance(v, dict)
            and all(
                isinstance(sub_k, int) and isinstance(sub_v, int)
                for sub_k, sub_v in v.items()
            )
            for k, v in l.items()
        ), "l is not Dict[int, Dict[int, int]]"

        # Strict type check for a generic dict
        assert isinstance(m, dict), "m is not dict"

        # Strict type check for FlyteFile
        assert isinstance(n, FlyteFile), "n is not FlyteFile"

        # Strict type check for FlyteDirectory
        assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

        # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

    @workflow
    def wf(bm: BM):
        t_inner(bm.inner_bm)
        t_test_all_attributes(
            a=bm.a,
            b=bm.b,
            c=bm.c,
            d=bm.d,
            e=bm.e,
            f=bm.f,
            g=bm.g,
            h=bm.h,
            i=bm.i,
            j=bm.j,
            k=bm.k,
            l=bm.l,
            m=bm.m,
            n=bm.n,
            o=bm.o,
            enum_status=bm.enum_status,
        )

    wf(bm=BM())


def test_all_types_with_optional_and_none_in_pydantic_basemodel_wf(
    local_dummy_file, local_dummy_directory
):
    class InnerBM(BaseModel):
        a: Optional[int] = None
        b: Optional[float] = None
        c: Optional[str] = None
        d: Optional[bool] = None
        e: Optional[List[int]] = None
        f: Optional[List[FlyteFile]] = None
        g: Optional[List[List[int]]] = None
        h: Optional[List[Dict[int, bool]]] = None
        i: Optional[Dict[int, bool]] = None
        j: Optional[Dict[int, FlyteFile]] = None
        k: Optional[Dict[int, List[int]]] = None
        l: Optional[Dict[int, Dict[int, int]]] = None
        m: Optional[dict] = None
        n: Optional[FlyteFile] = None
        o: Optional[FlyteDirectory] = None
        enum_status: Optional[Status] = None

    class BM(BaseModel):
        a: Optional[int] = None
        b: Optional[float] = None
        c: Optional[str] = None
        d: Optional[bool] = None
        e: Optional[List[int]] = None
        f: Optional[List[FlyteFile]] = None
        g: Optional[List[List[int]]] = None
        h: Optional[List[Dict[int, bool]]] = None
        i: Optional[Dict[int, bool]] = None
        j: Optional[Dict[int, FlyteFile]] = None
        k: Optional[Dict[int, List[int]]] = None
        l: Optional[Dict[int, Dict[int, int]]] = None
        m: Optional[dict] = None
        n: Optional[FlyteFile] = None
        o: Optional[FlyteDirectory] = None
        inner_bm: Optional[InnerBM] = None
        enum_status: Optional[Status] = None

    @task
    def t_inner(inner_bm: Optional[InnerBM]):
        return inner_bm

    @task
    def t_test_all_attributes(
        a: Optional[int],
        b: Optional[float],
        c: Optional[str],
        d: Optional[bool],
        e: Optional[List[int]],
        f: Optional[List[FlyteFile]],
        g: Optional[List[List[int]]],
        h: Optional[List[Dict[int, bool]]],
        i: Optional[Dict[int, bool]],
        j: Optional[Dict[int, FlyteFile]],
        k: Optional[Dict[int, List[int]]],
        l: Optional[Dict[int, Dict[int, int]]],
        m: Optional[dict],
        n: Optional[FlyteFile],
        o: Optional[FlyteDirectory],
        enum_status: Optional[Status],
    ):
        return

    @workflow
    def wf(bm: BM):
        t_inner(bm.inner_bm)
        t_test_all_attributes(
            a=bm.a,
            b=bm.b,
            c=bm.c,
            d=bm.d,
            e=bm.e,
            f=bm.f,
            g=bm.g,
            h=bm.h,
            i=bm.i,
            j=bm.j,
            k=bm.k,
            l=bm.l,
            m=bm.m,
            n=bm.n,
            o=bm.o,
            enum_status=bm.enum_status,
        )

    wf(bm=BM())


def test_input_from_flyte_console_pydantic_basemodel(
    local_dummy_file, local_dummy_directory
):
    # Flyte Console will send the input data as protobuf Struct

    class InnerBM(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = Field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: List[List[int]] = Field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = Field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = Field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = Field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = Field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = Field(default_factory=lambda: {1: {-1: 0}})
        m: dict = Field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = Field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        enum_status: Status = Field(default=Status.PENDING)

    class BM(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = Field(
            default_factory=lambda: [
                FlyteFile(local_dummy_file),
            ]
        )
        g: List[List[int]] = Field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = Field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = Field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = Field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = Field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = Field(default_factory=lambda: {1: {-1: 0}})
        m: dict = Field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = Field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = Field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())
        enum_status: Status = Field(default=Status.PENDING)

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

    def t_test_all_attributes(
        a: int,
        b: float,
        c: str,
        d: bool,
        e: List[int],
        f: List[FlyteFile],
        g: List[List[int]],
        h: List[Dict[int, bool]],
        i: Dict[int, bool],
        j: Dict[int, FlyteFile],
        k: Dict[int, List[int]],
        l: Dict[int, Dict[int, int]],
        m: dict,
        n: FlyteFile,
        o: FlyteDirectory,
        enum_status: Status,
    ):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(
            isinstance(i, int) for i in e
        ), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(
            isinstance(i, FlyteFile) for i in f
        ), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g
        ), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict)
            and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items())
            for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()
        ), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()
        ), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int)
            and isinstance(v, list)
            and all(isinstance(i, int) for i in v)
            for k, v in k.items()
        ), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int)
            and isinstance(v, dict)
            and all(
                isinstance(sub_k, int) and isinstance(sub_v, int)
                for sub_k, sub_v in v.items()
            )
            for k, v in l.items()
        ), "l is not Dict[int, Dict[int, int]]"

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
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct()))
    )

    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, BM
    )
    t_inner(downstream_input.inner_bm)
    t_test_all_attributes(
        a=downstream_input.a,
        b=downstream_input.b,
        c=downstream_input.c,
        d=downstream_input.d,
        e=downstream_input.e,
        f=downstream_input.f,
        g=downstream_input.g,
        h=downstream_input.h,
        i=downstream_input.i,
        j=downstream_input.j,
        k=downstream_input.k,
        l=downstream_input.l,
        m=downstream_input.m,
        n=downstream_input.n,
        o=downstream_input.o,
        enum_status=downstream_input.enum_status,
    )
    t_test_all_attributes(
        a=downstream_input.inner_bm.a,
        b=downstream_input.inner_bm.b,
        c=downstream_input.inner_bm.c,
        d=downstream_input.inner_bm.d,
        e=downstream_input.inner_bm.e,
        f=downstream_input.inner_bm.f,
        g=downstream_input.inner_bm.g,
        h=downstream_input.inner_bm.h,
        i=downstream_input.inner_bm.i,
        j=downstream_input.inner_bm.j,
        k=downstream_input.inner_bm.k,
        l=downstream_input.inner_bm.l,
        m=downstream_input.inner_bm.m,
        n=downstream_input.inner_bm.n,
        o=downstream_input.inner_bm.o,
        enum_status=downstream_input.inner_bm.enum_status,
    )


def test_flyte_types_deserialization_not_called_when_using_constructor(
    local_dummy_file, local_dummy_directory
):
    # Mocking both FlyteFilePathTransformer and FlyteDirectoryPathTransformer
    with patch(
        "flytekit.types.file.FlyteFilePathTransformer.to_python_value"
    ) as mock_file_to_python_value, patch(
        "flytekit.types.directory.FlyteDirToMultipartBlobTransformer.to_python_value"
    ) as mock_directory_to_python_value, patch(
        "flytekit.types.structured.StructuredDatasetTransformerEngine.to_python_value"
    ) as mock_structured_dataset_to_python_value, patch(
        "flytekit.types.schema.FlyteSchemaTransformer.to_python_value"
    ) as mock_schema_to_python_value:

        # Define your Pydantic model
        class BM(BaseModel):
            ff: FlyteFile = Field(default_factory=lambda: FlyteFile(local_dummy_file))
            fd: FlyteDirectory = Field(
                default_factory=lambda: FlyteDirectory(local_dummy_directory)
            )
            sd: StructuredDataset = Field(default_factory=lambda: StructuredDataset())
            fsc: FlyteSchema = Field(default_factory=lambda: FlyteSchema())

        # Create an instance of BM (should not call the deserialization)
        BM()

        mock_file_to_python_value.assert_not_called()
        mock_directory_to_python_value.assert_not_called()
        mock_structured_dataset_to_python_value.assert_not_called()
        mock_schema_to_python_value.assert_not_called()


def test_flyte_types_deserialization_called_once_when_using_model_validate_json(
    local_dummy_file, local_dummy_directory
):
    """
    It's hard to mock flyte schema and structured dataset in tests, so we will only test FlyteFile and FlyteDirectory
    """
    with patch(
        "flytekit.types.file.FlyteFilePathTransformer.to_python_value"
    ) as mock_file_to_python_value, patch(
        "flytekit.types.directory.FlyteDirToMultipartBlobTransformer.to_python_value"
    ) as mock_directory_to_python_value:
        # Define your Pydantic model
        class BM(BaseModel):
            ff: FlyteFile = Field(default_factory=lambda: FlyteFile(local_dummy_file))
            fd: FlyteDirectory = Field(
                default_factory=lambda: FlyteDirectory(local_dummy_directory)
            )

        # Create instances of FlyteFile and FlyteDirectory
        bm = BM(
            ff=FlyteFile(local_dummy_file), fd=FlyteDirectory(local_dummy_directory)
        )

        # Serialize and Deserialize with model_validate_json
        json_str = bm.model_dump_json()
        bm.model_validate_json(
            json_data=json_str, strict=False, context={"deserialize": True}
        )

        # Assert that the to_python_value method was called once
        mock_file_to_python_value.assert_called_once()
        mock_directory_to_python_value.assert_called_once()


def test_union_in_basemodel_wf():
    class bm(BaseModel):
        a: Union[int, bool, str, float]
        b: Union[int, bool, str, float]

    @task
    def add(
        a: Union[int, bool, str, float], b: Union[int, bool, str, float]
    ) -> Union[int, bool, str, float]:
        return a + b  # type: ignore

    @workflow
    def wf(bm: bm) -> Union[int, bool, str, float]:
        return add(bm.a, bm.b)

    assert wf(bm=bm(a=1, b=2)) == 3
    assert wf(bm=bm(a=True, b=False)) == True
    assert wf(bm=bm(a=False, b=False)) == False
    assert wf(bm=bm(a="hello", b="world")) == "helloworld"
    assert wf(bm=bm(a=1.0, b=2.0)) == 3.0

    @task
    def add_bm(bm1: bm, bm2: bm) -> Union[int, bool, str, float]:
        return bm1.a + bm2.b  # type: ignore

    @workflow
    def wf_add_bm(bm: bm) -> Union[int, bool, str, float]:
        return add_bm(bm, bm)

    assert wf_add_bm(bm=bm(a=1, b=2)) == 3

    @workflow
    def wf_return_bm(bm: bm) -> bm:
        return bm

    assert wf_return_bm(bm=bm(a=1, b=2)) == bm(a=1, b=2)


def test_basemodel_literal_type_annotation():
    class BM(BaseModel):
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"

    assert TypeEngine.to_literal_type(BM).annotation == TypeAnnotation({CACHE_KEY_METADATA: {SERIALIZATION_FORMAT: MESSAGEPACK}})


@mock.patch("flytekit.remote.remote_fs.FlytePathResolver")
def test_modify_literal_uris_call(mock_resolver):
    ctx = FlyteContextManager.current_context()

    sd = StructuredDataset(dataframe=pd.DataFrame(
        {"a": [1, 2], "b": [3, 4]}))

    class BM(BaseModel):
        s: StructuredDataset

    bm = BM(s=sd)

    def mock_resolve_remote_path(flyte_uri: str):
        p = Path(flyte_uri)
        if p.exists():
            return "/my/replaced/val"
        return ""

    mock_resolver.resolve_remote_path.side_effect = mock_resolve_remote_path
    mock_resolver.protocol = "/"

    lt = TypeEngine.to_literal_type(BM)
    lit = TypeEngine.to_literal(ctx, bm, BM, lt)

    bm_revived = TypeEngine.to_python_value(ctx, lit, BM)
    assert bm_revived.s.literal.uri == "/my/replaced/val"

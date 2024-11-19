import sys
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List

import pytest
from flytekit import task, workflow
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
from flytekit.types.structured import StructuredDataset
from flytekit.types.schema import FlyteSchema
from flytekit import task, workflow
from enum import Enum


@pytest.fixture
def local_tmp_file():
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".txt") as tmp_file:
        tmp_file.write("Hello World!")
        tmp_file.flush()
        tmp_file_path = tmp_file.name

        yield tmp_file_path


@pytest.fixture
def local_tmp_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        with open(Path(tmp_dir) / "example.txt", "w") as f:
            f.write("Hello World!")

        yield tmp_dir


@pytest.fixture
def local_pqt_file():
    return Path(__file__).parents[2] / "integration/remote/workflows/basic/data/df.parquet"


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
def test_all_dc_attrs(local_tmp_file, local_tmp_dir, local_pqt_file):
    import os
    import pandas as pd

    # Enable generating protobuf struct in the generic IDL
    # Please refer to https://github.com/flyteorg/flyte/issues/5959
    os.environ["FLYTE_USE_OLD_DC_FORMAT"] = "True"


    class Status(Enum):
        PENDING = "pending"
        APPROVED = "approved"
        REJECTED = "rejected"

    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile(local_tmp_file)])
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
        i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
        j: Dict[int, FlyteFile] = field(default_factory=lambda: {
            0: FlyteFile(local_tmp_file), 1: FlyteFile(local_tmp_file), -1: FlyteFile(local_tmp_file)
        })
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_tmp_file))
        o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_tmp_dir))
        enum_status: Status = field(default=Status.PENDING)
        sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(uri=local_pqt_file, file_format="parquet"))
        fsc: FlyteSchema = field(default_factory=lambda: FlyteSchema(local_path=local_pqt_file))

    @dataclass
    class DC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile(local_tmp_file)])
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
        i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
        j: Dict[int, FlyteFile] = field(default_factory=lambda: {
            0: FlyteFile(local_tmp_file), 1: FlyteFile(local_tmp_file), -1: FlyteFile(local_tmp_file)
        })
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_tmp_file))
        o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory(local_tmp_dir))
        enum_status: Status = field(default=Status.PENDING)
        sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(uri=local_pqt_file, file_format="parquet"))
        fsc: FlyteSchema = field(default_factory=lambda: FlyteSchema(local_path=local_pqt_file))

        # Define a nested dataclass
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())

    @task
    def t_dc(dc: DC) -> DC:
        assert isinstance(dc, DC), "dc is not of type DC"

        return dc

    @task
    def t_inner(inner_dc: InnerDC) -> InnerDC:
        assert isinstance(inner_dc, InnerDC), "inner_dc is not of type InnerDC"

        # f: List[FlyteFile]
        for ff in inner_dc.f:
            assert isinstance(ff, FlyteFile), "Expected FlyteFile"
            with open(ff, "r") as f:
                assert f.read() == EXPECTED_FILE_CONTENT, "File content mismatch in f"

        # j: Dict[int, FlyteFile]
        for _, ff in inner_dc.j.items():
            assert isinstance(ff, FlyteFile), "Expected FlyteFile in j"
            with open(ff, "r") as f:
                assert f.read() == EXPECTED_FILE_CONTENT, "File content mismatch in j"

        # n: FlyteFile
        assert isinstance(inner_dc.n, FlyteFile), "n is not FlyteFile"
        with open(inner_dc.n, "r") as f:
            assert f.read() == EXPECTED_FILE_CONTENT, "File content mismatch in n"

        # o: FlyteDirectory
        assert isinstance(inner_dc.o, FlyteDirectory), "o is not FlyteDirectory"
        assert not inner_dc.o.downloaded, "o should not be downloaded initially"
        with open(os.path.join(inner_dc.o, "example.txt"), "r") as fh:
            assert fh.read() == EXPECTED_FILE_CONTENT, "File content mismatch in o"
        assert inner_dc.o.downloaded, "o should be marked as downloaded after access"

        assert inner_dc.enum_status == Status.PENDING, "enum_status does not match"
        assert isinstance(inner_dc.sd, StructuredDataset), "sd is not StructuredDataset"
        assert isinstance(inner_dc.fsc, FlyteSchema), "fsc is not FlyteSchema"
        print("All checks in InnerDC passed")

        return inner_dc

    @task
    def t_test_all_attributes(
        a: int, b: float, c: str, d: bool,
        e: List[int], f: List[FlyteFile], g: List[List[int]], h: List[Dict[int, bool]],
        i: Dict[int, bool], j: Dict[int, FlyteFile], k: Dict[int, List[int]], l: Dict[int, Dict[int, int]],
        m: dict, n: FlyteFile, o: FlyteDirectory, enum_status: Status,
        sd: StructuredDataset, fsc: FlyteSchema
    ) -> None:
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

        # # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

        assert isinstance(sd, StructuredDataset), "sd is not StructuredDataset"
        print("sd:", sd.open(pd.DataFrame).all())

        assert isinstance(fsc, FlyteSchema), "fsc is not FlyteSchema"
        print("fsc: ", fsc.open().all())

        print("All attributes passed strict type checks.")


    @workflow
    def wf(dc: DC) -> None:
        new_dc = t_dc(dc=dc)
        t_inner(new_dc.inner_dc)

        # Test outer dc
        t_test_all_attributes(
            a=new_dc.a, b=new_dc.b, c=new_dc.c, d=new_dc.d,
            e=new_dc.e, f=new_dc.f, g=new_dc.g, h=new_dc.h,
            i=new_dc.i, j=new_dc.j, k=new_dc.k, l=new_dc.l,
            m=new_dc.m, n=new_dc.n, o=new_dc.o, enum_status=new_dc.enum_status,
            sd=new_dc.sd, fsc=new_dc.fsc
        )

        # Test inner dc
        t_test_all_attributes(
            a=new_dc.inner_dc.a, b=new_dc.inner_dc.b, c=new_dc.inner_dc.c, d=new_dc.inner_dc.d,
            e=new_dc.inner_dc.e, f=new_dc.inner_dc.f, g=new_dc.inner_dc.g, h=new_dc.inner_dc.h,
            i=new_dc.inner_dc.i, j=new_dc.inner_dc.j, k=new_dc.inner_dc.k, l=new_dc.inner_dc.l,
            m=new_dc.inner_dc.m, n=new_dc.inner_dc.n, o=new_dc.inner_dc.o, enum_status=new_dc.inner_dc.enum_status,
            sd=new_dc.inner_dc.sd, fsc=new_dc.inner_dc.fsc
        )


    EXPECTED_FILE_CONTENT = "Hello World!"

    wf(dc=DC())


def test_mini_dc_attrs():
    """
    Test dc attributes which focuses only on protobuf structs.

    This test doesn't depend on pandas.
    """
    import os

    # Enable generating protobuf struct in the generic IDL
    # Please refer to https://github.com/flyteorg/flyte/issues/5959
    os.environ["FLYTE_USE_OLD_DC_FORMAT"] = "True"

    @dataclass
    class InnerDC:
        a: int = -1
        b: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        c: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})

    @dataclass
    class DC:
        a: int = -1
        b: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        c: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})

        # Define a nested dataclass
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())

    @task
    def t_test_attrs(a: int, b: List[int], c: Dict[int, bool]) -> None:
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1

        assert isinstance(b, list) and all(isinstance(i, int) for i in b), "b is not List[int]"

        assert isinstance(c, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in c.items()
        ), "c is not Dict[int, bool]"

    @workflow
    def wf(dc: DC) -> None:
        # Test outer dc
        t_test_attrs(dc.a, dc.b, dc.c)

        # Test inner dc
        t_test_attrs(dc.inner_dc.a, dc.inner_dc.b, dc.inner_dc.c)


    wf(dc=DC())

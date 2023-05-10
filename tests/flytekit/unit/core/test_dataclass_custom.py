from __future__ import annotations

import os
import tempfile
import typing
from dataclasses import dataclass, field

import pytest
from dataclasses_json import dataclass_json

from flytekit.core.context_manager import FlyteContext
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.core.workflow import workflow
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer


# Fixture that ensures a dummy local file
@pytest.fixture
def local_dummy_file():
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.write("Hello world")
        yield path
    finally:
        os.remove(path)


@dataclass_json
@dataclass
class FlyteFileCustom(FlyteFile):
    custom_field: typing.List[str] = field(default_factory=list)


@dataclass_json
@dataclass
class FileWrapper(object):
    wrapped: FlyteFileCustom


class FlyteFileCustomTransformer(FlyteFilePathTransformer):
    def __init__(self) -> None:
        TypeTransformer.__init__(self, name="FlyteFileCustom", t=FlyteFileCustom)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: typing.Union[
            typing.Type[FlyteFileCustom], os.PathLike
        ],
    ) -> FlyteFileCustom:
        python_value = super().to_python_value(ctx, lv, expected_python_type)
        ffc = FlyteFileCustom(python_value.path, python_value._downloader, python_value.remote_path)
        ffc._remote_source = python_value._remote_source
        ffc.custom_field = lv.hash.split(",")

        return ffc

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: typing.Union[FlyteFileCustom, os.PathLike, str],
        python_type: typing.Type[FlyteFileCustom],
        expected: LiteralType,
    ) -> Literal:
        literal = super().to_literal(ctx, python_val, python_type, expected)
        literal.hash = ",".join(python_val.custom_field)
        return literal


TypeEngine.register(FlyteFileCustomTransformer())


def test_basic_custom_file():
    SAMPLE_DATA = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"

    @task
    def t1() -> FlyteFileCustom:
        ffc = FlyteFileCustom(SAMPLE_DATA)
        ffc.custom_field = ["hello", "world"]
        return ffc

    @workflow
    def my_wf() -> FlyteFileCustom:
        return t1()

    # This creates a random directory that we know is empty.
    random_dir = FlyteContextManager.current_context().file_access.get_random_local_directory()
    # Creating a new FileAccessProvider will add two folders to the random dir
    print(f"Random {random_dir}")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=os.path.join(random_dir, "mock_remote"))
    ctx = FlyteContextManager.current_context()
    with FlyteContextManager.with_context(ctx.with_file_access(fs)):
        working_dir = os.listdir(random_dir)
        assert len(working_dir) == 1  # the local_flytekit folder

        workflow_output = my_wf()

        # After running the mock remote dir should still be empty, since the workflow_output has not been used
        with pytest.raises(FileNotFoundError):
            os.listdir(os.path.join(random_dir, "mock_remote"))

        # While the literal returned by t1 does contain the web address as the uri, because it's a remote address,
        # flytekit will translate it back into a FlyteFile object on the local drive (but not download it)
        assert workflow_output.path.startswith(random_dir)
        # But the remote source should still be the https address
        assert workflow_output.remote_source == SAMPLE_DATA
        assert workflow_output.custom_field == ["hello", "world"]


def test_custom_file_in_dataclass():
    SAMPLE_DATA = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"

    @task
    def t1() -> FileWrapper:
        ffc = FlyteFileCustom(SAMPLE_DATA)
        ffc.custom_field = ["hello", "world"]
        return FileWrapper(ffc)

    @task
    def t2(a: FileWrapper) -> str:
        return " ".join(a.wrapped.custom_field)

    @workflow
    def my_wf() -> str:
        x = t1()
        return t2(a=x)

    # This creates a random directory that we know is empty.
    random_dir = FlyteContextManager.current_context().file_access.get_random_local_directory()
    # Creating a new FileAccessProvider will add two folders to the random dir
    print(f"Random {random_dir}")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=os.path.join(random_dir, "mock_remote"))
    ctx = FlyteContextManager.current_context()
    with FlyteContextManager.with_context(ctx.with_file_access(fs)):
        working_dir = os.listdir(random_dir)
        assert len(working_dir) == 1  # the local_flytekit folder

        workflow_output = my_wf()


        print('hi')


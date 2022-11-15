import os
import pathlib
import tempfile
import typing
from unittest.mock import MagicMock

import pytest

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.core.context_manager import ExecutionState
from flytekit.core.data_persistence import FileAccessProvider, flyte_tmp_dir
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import workflow
from flytekit.models.core.types import BlobType
from flytekit.models.literals import LiteralMap
from flytekit.types.file.file import FlyteFile


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


def test_file_type_in_workflow_with_bad_format():
    @task
    def t1() -> FlyteFile[typing.TypeVar("txt")]:
        fname = "/tmp/flytekit_test"
        with open(fname, "w") as fh:
            fh.write("Hello World\n")
        return fname

    @workflow
    def my_wf() -> FlyteFile[typing.TypeVar("txt")]:
        f = t1()
        return f

    res = my_wf()
    with open(res, "r") as fh:
        assert fh.read() == "Hello World\n"


def test_file_handling_remote_default_wf_input():
    SAMPLE_DATA = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"

    @task
    def t1(fname: os.PathLike) -> int:
        with open(fname, "r") as fh:
            x = len(fh.readlines())

        return x

    @workflow
    def my_wf(fname: os.PathLike = SAMPLE_DATA) -> int:
        length = t1(fname=fname)
        return length

    assert my_wf.python_interface.inputs_with_defaults["fname"][1] == SAMPLE_DATA
    sample_lp = LaunchPlan.create("test_launch_plan", my_wf)
    assert sample_lp.parameters.parameters["fname"].default.scalar.blob.uri == SAMPLE_DATA


def test_file_handling_local_file_gets_copied():
    @task
    def t1() -> FlyteFile:
        # Use this test file itself, since we know it exists.
        return __file__

    @workflow
    def my_wf() -> FlyteFile:
        return t1()

    random_dir = context_manager.FlyteContext.current_context().file_access.get_random_local_directory()
    # print(f"Random: {random_dir}")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=os.path.join(random_dir, "mock_remote"))
    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_file_access(fs)):
        top_level_files = os.listdir(random_dir)
        assert len(top_level_files) == 1  # the flytekit_local folder

        x = my_wf()

        # After running, this test file should've been copied to the mock remote location.
        mock_remote_files = os.listdir(os.path.join(random_dir, "mock_remote"))
        assert len(mock_remote_files) == 1  # the file
        # File should've been copied to the mock remote folder
        assert x.path.startswith(random_dir)


def test_file_handling_local_file_gets_force_no_copy():
    @task
    def t1() -> FlyteFile:
        # Use this test file itself, since we know it exists.
        return FlyteFile(__file__, remote_path=False)

    @workflow
    def my_wf() -> FlyteFile:
        return t1()

    random_dir = context_manager.FlyteContext.current_context().file_access.get_random_local_directory()
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=os.path.join(random_dir, "mock_remote"))
    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_file_access(fs)):
        top_level_files = os.listdir(random_dir)
        assert len(top_level_files) == 1  # the flytekit_local folder

        workflow_output = my_wf()

        # After running, this test file should've been copied to the mock remote location.
        assert not os.path.exists(os.path.join(random_dir, "mock_remote"))

        # Because Flyte doesn't presume to handle a uri that look like a raw path, the path that is returned is
        # the original.
        assert workflow_output.path == __file__


def test_file_handling_remote_file_handling():
    SAMPLE_DATA = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"

    @task
    def t1() -> FlyteFile:
        return SAMPLE_DATA

    @workflow
    def my_wf() -> FlyteFile:
        return t1()

    # This creates a random directory that we know is empty.
    random_dir = context_manager.FlyteContext.current_context().file_access.get_random_local_directory()
    # Creating a new FileAccessProvider will add two folderst to the random dir
    print(f"Random {random_dir}")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=os.path.join(random_dir, "mock_remote"))
    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_file_access(fs)):
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

        # The act of running the workflow should create the engine dir, and the directory that will contain the
        # file but the file itself isn't downloaded yet.
        working_dir = os.listdir(os.path.join(random_dir, "local_flytekit"))
        # This second layer should have two dirs, a random one generated by the new_execution_context call
        # and an empty folder, created by FlyteFile transformer's to_python_value function. This folder will have
        # something in it after we open() it.
        assert len(working_dir) == 1

        assert not os.path.exists(workflow_output.path)
        # # The act of opening it should trigger the download, since we do lazy downloading.
        with open(workflow_output, "rb"):
            ...
        # assert os.path.exists(workflow_output.path)
        #
        # # The file name is maintained on download.
        # assert str(workflow_output).endswith(os.path.split(SAMPLE_DATA)[1])


def test_file_handling_remote_file_handling_flyte_file():
    SAMPLE_DATA = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"

    @task
    def t1() -> FlyteFile:
        # Unlike the test above, this returns the remote path wrapped in a FlyteFile object
        return FlyteFile(SAMPLE_DATA)

    @workflow
    def my_wf() -> FlyteFile:
        return t1()

    # This creates a random directory that we know is empty.
    random_dir = context_manager.FlyteContext.current_context().file_access.get_random_local_directory()
    # Creating a new FileAccessProvider will add two folderst to the random dir
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=os.path.join(random_dir, "mock_remote"))
    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_file_access(fs)):
        working_dir = os.listdir(random_dir)
        assert len(working_dir) == 1  # the local_flytekit dir

        mock_remote_path = os.path.join(random_dir, "mock_remote")
        assert not os.path.exists(mock_remote_path)  # the persistence layer won't create the folder yet

        workflow_output = my_wf()

        # After running the mock remote dir should still be empty, since the workflow_output has not been used
        assert not os.path.exists(mock_remote_path)

        # While the literal returned by t1 does contain the web address as the uri, because it's a remote address,
        # flytekit will translate it back into a FlyteFile object on the local drive (but not download it)
        assert workflow_output.path.startswith(f"{random_dir}{os.sep}local_flytekit")
        # But the remote source should still be the https address
        assert workflow_output.remote_source == SAMPLE_DATA

        # The act of running the workflow should create the engine dir, and the directory that will contain the
        # file but the file itself isn't downloaded yet.
        working_dir = os.listdir(os.path.join(random_dir, "local_flytekit"))
        assert len(working_dir) == 1  # local flytekit and the downloaded file

        assert not os.path.exists(workflow_output.path)
        # # The act of opening it should trigger the download, since we do lazy downloading.
        with open(workflow_output, "rb"):
            ...
        # This second layer should have two dirs, a random one generated by the new_execution_context call
        # and an empty folder, created by FlyteFile transformer's to_python_value function. This folder will have
        # something in it after we open() it.
        working_dir = os.listdir(os.path.join(random_dir, "local_flytekit"))
        assert len(working_dir) == 2  # local flytekit and the downloaded file

        assert os.path.exists(workflow_output.path)

        # The file name is maintained on download.
        assert str(workflow_output).endswith(os.path.split(SAMPLE_DATA)[1])


def test_dont_convert_remotes():
    @task
    def t1(in1: FlyteFile):
        print(in1)

    @dynamic
    def dyn(in1: FlyteFile):
        t1(in1=in1)

    fd = FlyteFile("s3://anything")

    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_serialization_settings(
            flytekit.configuration.SerializationSettings(
                project="test_proj",
                domain="test_domain",
                version="abc",
                image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
                env={},
            )
        )
    ):
        ctx = context_manager.FlyteContextManager.current_context()
        with context_manager.FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.new_execution_state().with_params(mode=ExecutionState.Mode.TASK_EXECUTION))
        ) as ctx:
            lit = TypeEngine.to_literal(
                ctx, fd, FlyteFile, BlobType("", dimensionality=BlobType.BlobDimensionality.SINGLE)
            )
            lm = LiteralMap(literals={"in1": lit})
            wf = dyn.dispatch_execute(ctx, lm)
            assert wf.nodes[0].inputs[0].binding.scalar.blob.uri == "s3://anything"

            with pytest.raises(TypeError, match="No automatic conversion found from type <class 'int'>"):
                TypeEngine.to_literal(
                    ctx, 3, FlyteFile, BlobType("", dimensionality=BlobType.BlobDimensionality.SINGLE)
                )


def test_download_caching():
    mock_downloader = MagicMock()
    f = FlyteFile("test", mock_downloader)
    assert not f.downloaded
    os.fspath(f)
    assert f.downloaded
    assert mock_downloader.call_count == 1
    for _ in range(10):
        os.fspath(f)
    assert mock_downloader.call_count == 1


def test_returning_a_pathlib_path(local_dummy_file):
    @task
    def t1() -> FlyteFile:
        return pathlib.Path(local_dummy_file)

    # TODO: Remove this - only here to trigger type engine
    @workflow
    def wf1() -> FlyteFile:
        return t1()

    wf_out = wf1()
    assert isinstance(wf_out, FlyteFile)
    with open(wf_out, "r") as fh:
        assert fh.read() == "Hello world"
    assert wf_out._downloaded

    # Remove the file, then call download again, it should not because _downloaded was already set.
    os.remove(wf_out.path)
    p = wf_out.download()
    assert not os.path.exists(wf_out.path)
    assert p == wf_out.path

    @task
    def t2() -> os.PathLike:
        return pathlib.Path(local_dummy_file)

    # TODO: Remove this
    @workflow
    def wf2() -> os.PathLike:
        return t2()

    wf_out = wf2()
    assert isinstance(wf_out, FlyteFile)
    with open(wf_out, "r") as fh:
        assert fh.read() == "Hello world"


def test_output_type_pathlike(local_dummy_file):
    @task
    def t1() -> os.PathLike:
        return FlyteFile(local_dummy_file)

    # TODO: Remove this - only here to trigger type engine
    @workflow
    def wf1() -> os.PathLike:
        return t1()

    wf_out = wf1()
    assert isinstance(wf_out, FlyteFile)
    with open(wf_out, "r") as fh:
        assert fh.read() == "Hello world"


def test_input_type_pathlike(local_dummy_file):
    @task
    def t1(a: os.PathLike):
        assert isinstance(a, FlyteFile)
        with open(a, "r") as fh:
            assert fh.read() == "Hello world"

    # TODO: Remove this - only here to trigger type engine
    @workflow
    def my_wf(a: FlyteFile):
        t1(a=a)

    my_wf(a=local_dummy_file)


def test_returning_folder_instead_of_file():
    @task
    def t1() -> FlyteFile:
        return pathlib.Path(tempfile.gettempdir())

    # TODO: Remove this - only here to trigger type engine
    @workflow
    def wf1() -> FlyteFile:
        return t1()

    with pytest.raises(TypeError):
        wf1()

    @task
    def t2() -> FlyteFile:
        return tempfile.gettempdir()

    # TODO: Remove this - only here to trigger type engine
    @workflow
    def wf2() -> FlyteFile:
        return t2()

    with pytest.raises(TypeError):
        wf2()


def test_bad_return():
    @task
    def t1() -> FlyteFile:
        return 1

    # TODO: Remove this - only here to trigger type engine
    @workflow
    def wf1() -> FlyteFile:
        return t1()

    with pytest.raises(TypeError):
        wf1()


def test_file_guess():
    transformer = TypeEngine.get_transformer(FlyteFile)
    lt = transformer.get_literal_type(FlyteFile["txt"])
    assert lt.blob.format == "txt"
    assert lt.blob.dimensionality == 0

    fft = transformer.guess_python_type(lt)
    assert issubclass(fft, FlyteFile)
    assert fft.extension() == "txt"

    lt = transformer.get_literal_type(FlyteFile)
    assert lt.blob.format == ""
    assert lt.blob.dimensionality == 0

    fft = transformer.guess_python_type(lt)
    assert issubclass(fft, FlyteFile)
    assert fft.extension() == ""


def test_flyte_file_in_dyn():
    @task
    def t1(path: str) -> FlyteFile:
        return FlyteFile(path)

    @dynamic
    def dyn(fs: FlyteFile):
        t2(ff=fs)

    @task
    def t2(ff: FlyteFile) -> os.PathLike:
        assert ff.remote_source == "s3://somewhere"
        assert flyte_tmp_dir in ff.path

        return ff.path

    @workflow
    def wf(path: str) -> os.PathLike:
        n1 = t1(path=path)
        dyn(fs=n1)
        return t2(ff=n1)

    assert flyte_tmp_dir in wf(path="s3://somewhere").path

import mock

from flytekit import FlyteContext, FlyteContextManager
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


def test_new_file_dir():
    fd = FlyteDirectory(path="s3://my-bucket")
    assert fd.sep == "/"
    inner_dir = fd.new_dir("test")
    assert inner_dir.path == "s3://my-bucket/test"
    fd = FlyteDirectory(path="s3://my-bucket/")
    inner_dir = fd.new_dir("test")
    assert inner_dir.path == "s3://my-bucket/test"
    f = inner_dir.new_file("test")
    assert isinstance(f, FlyteFile)
    assert f.path == "s3://my-bucket/test/test"


def test_new_remote_dir():
    fd = FlyteDirectory.new_remote()
    assert FlyteContext.current_context().file_access.raw_output_prefix in fd.path

def test_new_remote_dir_alt():
    ff = FlyteDirectory.new_remote(alt="my-alt-bucket", stem="my-stem")
    assert "my-alt-bucket" in ff.path
    assert "my-stem" in ff.path

def test_new_auto_new_dir():
    fd = FlyteDirectory.new("my_dir")
    assert FlyteContextManager.current_context().user_space_params.working_directory in fd.path

def test_add_path_to_dir():
    fd = FlyteDirectory.new("my_other_dir")
    cwd = FlyteContextManager.current_context().user_space_params.working_directory
    assert cwd in str(fd / "myfile.txt")


@mock.patch("flytekit.types.directory.types.os.name", "nt")
def test_sep_nt():
    fd = FlyteDirectory(path="file://mypath")
    assert fd.sep == "\\"
    fd = FlyteDirectory(path="s3://mypath")
    assert fd.sep == "/"

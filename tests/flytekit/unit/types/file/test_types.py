from flytekit.types.file import FlyteFile
from flytekit import FlyteContextManager

def test_new_remote_alt():
    ff = FlyteFile.new_remote_file(alt="my-alt-prefix", name="my-file.txt")
    assert "my-alt-prefix" in ff.path
    assert "my-file.txt" in ff.path

def test_new_auto_file():
    ff = FlyteFile.new("my-file.txt")
    cwd = FlyteContextManager.current_context().user_space_params.working_directory
    assert cwd in ff.path

def test_file_is_hashable():

    ff1 = FlyteFile.new("file1.txt")
    ff2 = FlyteFile.new("file2.txt")

    assert isinstance({ff1, ff2}, set)

    assert isinstance(hash(ff1), int)

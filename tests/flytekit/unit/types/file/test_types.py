from flytekit.types.file import FlyteFile
from flytekit import FlyteContextManager
from flytekit import FlyteContext

def test_new_remote_alt():
    ff = FlyteFile.new_remote_file(alt="my-alt-prefix", name="my-file.txt")
    assert "my-alt-prefix" in ff.path
    assert "my-file.txt" in ff.path

def test_new_auto_file():
    ff = FlyteFile.new("my-file.txt")
    cwd = FlyteContextManager.current_context().user_space_params.working_directory
    assert cwd in ff.path

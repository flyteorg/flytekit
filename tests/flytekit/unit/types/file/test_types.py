from flytekit.types.file import FlyteFile
from flytekit import FlyteContextManager

def test_new_remote_alt():
    ff = FlyteFile.new_remote_file(alt="my-alt-prefix")
    assert "my-alt-prefix" in ff.path

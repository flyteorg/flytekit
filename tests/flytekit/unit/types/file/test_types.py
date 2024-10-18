from pathlib import Path

from flytekit.types.file import FlyteFile
from flytekit import FlyteContextManager

def test_new_remote_alt():
    ff = FlyteFile.new_remote_file(alt="my-alt-prefix", name="my-file.txt")
    assert "my-alt-prefix" in ff.path
    assert "my-file.txt" in ff.path

def test_new_remote_from_pathlib():
    ff = FlyteFile(path=Path("my-file.txt"))

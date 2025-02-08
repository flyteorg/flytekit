import tempfile
from pathlib import Path

from flytekit import FlyteDirectory, FlyteFile, map, task, workflow

def test_listdir():
    @task
    def setup() -> FlyteDirectory:
        tmpdir = Path(tempfile.mkdtemp())
        (tmpdir / "file.txt").write_text("Hello, World!")
        return FlyteDirectory(tmpdir)


    @task
    def read_file(file: FlyteFile) -> str:
        with open(file, "r") as f:
            return f.read()


    @task
    def list_dir(dir: FlyteDirectory) -> list[FlyteFile]:
        return FlyteDirectory.listdir(dir)


    @workflow
    def wf() -> list[str]:
        tmpdir = setup()
        files = list_dir(dir=tmpdir)
        return map(read_file)(file=files)

    assert wf() == ["Hello, World!"]

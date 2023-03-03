from pathlib import Path

from flytekit import FlyteContext
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


def test_crawl(tmpdir):
    td_path = Path(tmpdir)
    test_dir = td_path.joinpath("test")
    test_dir.mkdir()
    f = test_dir.joinpath("file.txt")
    with f.open("w") as fp:
        fp.write("blah")
    fd = FlyteDirectory(path=str(td_path))
    l = list(fd.crawl())
    assert len(l) == 1
    assert l[0] == (str(tmpdir), "test/file.txt")

    l = list(fd.crawl(detail=True))
    assert len(l) == 1
    assert l[0][0] == str(tmpdir)
    assert isinstance(l[0][1], dict)
    assert "test/file.txt" in l[0][1]


def test_new_file_dir():
    fd = FlyteDirectory(path="s3://my-bucket")
    inner_dir = fd.new_dir("test")
    assert inner_dir.path == "s3://my-bucket/test"
    f = inner_dir.new_file("test")
    assert isinstance(f, FlyteFile)
    assert f.path == "s3://my-bucket/test/test"


def test_new_remote_dir():
    fd = FlyteDirectory.new_remote()
    assert FlyteContext.current_context().file_access.raw_output_prefix in fd.path

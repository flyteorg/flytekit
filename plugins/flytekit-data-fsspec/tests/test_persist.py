import os
import pathlib
import tempfile

from flytekitplugins.fsspec.persist import FSSpecPersistence, s3_setup_args
from fsspec.implementations.local import LocalFileSystem

from flytekit.configuration import S3Config


def test_s3_setup_args():
    kwargs = s3_setup_args(S3Config())
    assert kwargs == {}

    kwargs = s3_setup_args(S3Config(endpoint="http://localhost:30084"))
    assert kwargs == {"client_kwargs": {"endpoint_url": "http://localhost:30084"}}

    kwargs = s3_setup_args(S3Config(access_key_id="access"))
    assert kwargs == {"key": "access"}


def test_get_protocol():
    assert FSSpecPersistence.get_protocol("s3://abc") == "s3"
    assert FSSpecPersistence.get_protocol("/abc") == "file"
    assert FSSpecPersistence.get_protocol("file://abc") == "file"
    assert FSSpecPersistence.get_protocol("gs://abc") == "gs"
    assert FSSpecPersistence.get_protocol("sftp://abc") == "sftp"
    assert FSSpecPersistence.get_protocol("abfs://abc") == "abfs"


def test_get_anonymous_filesystem():
    fp = FSSpecPersistence()
    fs = fp.get_anonymous_filesystem("/abc")
    assert fs is None
    fs = fp.get_anonymous_filesystem("s3://abc")
    assert fs is not None
    assert fs.protocol == ["s3", "s3a"]


def test_get_filesystem():
    fp = FSSpecPersistence()
    fs = fp.get_filesystem("/abc")
    assert fs is not None
    assert isinstance(fs, LocalFileSystem)


def test_recursive_paths():
    f, t = FSSpecPersistence.recursive_paths("/tmp", "/tmp")
    assert (f, t) == ("/tmp/*", "/tmp/")
    f, t = FSSpecPersistence.recursive_paths("/tmp/", "/tmp/")
    assert (f, t) == ("/tmp/*", "/tmp/")
    f, t = FSSpecPersistence.recursive_paths("/tmp/*", "/tmp")
    assert (f, t) == ("/tmp/*", "/tmp/")


def test_exists():
    fs = FSSpecPersistence()
    assert not fs.exists("/tmp/non-existent")

    with tempfile.TemporaryDirectory() as tdir:
        f = os.path.join(tdir, "f.txt")
        with open(f, "w") as fp:
            fp.write("hello")

        assert fs.exists(f)


def test_get():
    fs = FSSpecPersistence()
    with tempfile.TemporaryDirectory() as tdir:
        f = os.path.join(tdir, "f.txt")
        with open(f, "w") as fp:
            fp.write("hello")

        t = os.path.join(tdir, "t.txt")

        fs.get(f, t)
        with open(t, "r") as fp:
            assert fp.read() == "hello"


def test_get_recursive():
    fs = FSSpecPersistence()
    with tempfile.TemporaryDirectory() as tdir:
        p = pathlib.Path(tdir)
        d = p.joinpath("d")
        d.mkdir()
        f = d.joinpath(d, "f.txt")
        with open(f, "w") as fp:
            fp.write("hello")

        o = p.joinpath("o")

        t = o.joinpath(o, "f.txt")
        fs.get(str(d), str(o), recursive=True)
        with open(t, "r") as fp:
            assert fp.read() == "hello"


def test_put():
    fs = FSSpecPersistence()
    with tempfile.TemporaryDirectory() as tdir:
        f = os.path.join(tdir, "f.txt")
        with open(f, "w") as fp:
            fp.write("hello")

        t = os.path.join(tdir, "t.txt")

        fs.put(f, t)
        with open(t, "r") as fp:
            assert fp.read() == "hello"


def test_put_recursive():
    fs = FSSpecPersistence()
    with tempfile.TemporaryDirectory() as tdir:
        p = pathlib.Path(tdir)
        d = p.joinpath("d")
        d.mkdir()
        f = d.joinpath(d, "f.txt")
        with open(f, "w") as fp:
            fp.write("hello")

        o = p.joinpath("o")

        t = o.joinpath(o, "f.txt")
        fs.put(str(d), str(o), recursive=True)
        with open(t, "r") as fp:
            assert fp.read() == "hello"


def test_construct_path():
    fs = FSSpecPersistence()
    assert fs.construct_path(True, False, "abc") == "file://abc"

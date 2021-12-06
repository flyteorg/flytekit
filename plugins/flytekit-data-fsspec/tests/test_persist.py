import os
import pathlib
import tempfile

from flytekitplugins.fsspec.persist import FSSpecPersistence, s3_setup_args
from fsspec.implementations.local import LocalFileSystem

from flytekit.configuration import aws


def test_s3_setup_args():
    kwargs = s3_setup_args()
    assert kwargs == {}

    with aws.S3_ENDPOINT.get_patcher("http://localhost:30084"):
        kwargs = s3_setup_args()
        assert kwargs == {"client_kwargs": {"endpoint_url": "http://localhost:30084"}}

    with aws.S3_ACCESS_KEY_ID.get_patcher("access"):
        kwargs = s3_setup_args()
        assert kwargs == {}
        assert os.environ[aws.S3_ACCESS_KEY_ID_ENV_NAME] == "access"


def test_get_protocol():
    assert FSSpecPersistence._get_protocol("s3://abc") == "s3"
    assert FSSpecPersistence._get_protocol("/abc") == "file"
    assert FSSpecPersistence._get_protocol("file://abc") == "file"
    assert FSSpecPersistence._get_protocol("gs://abc") == "gs"
    assert FSSpecPersistence._get_protocol("sftp://abc") == "sftp"
    assert FSSpecPersistence._get_protocol("abfs://abc") == "abfs"


def test_get_filesystem():
    fs = FSSpecPersistence._get_filesystem("/abc")
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

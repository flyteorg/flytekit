import os

import pytest

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types.impl import blobs
from flytekit.common.utils import AutoDeletingTempDir
from flytekit.models.core import types as _core_types
from flytekit.sdk import test_utils


def test_blob():
    b = blobs.Blob("/tmp/fake")
    assert b.remote_location == "/tmp/fake"
    assert b.local_path is None
    assert b.mode == "rb"
    assert b.metadata.type.format == ""
    assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE


def test_blob_from_python_std():
    with test_utils.LocalTestFileSystem() as t:
        with AutoDeletingTempDir("test") as wd:
            tmp_name = wd.get_named_tempfile("from_python_std")
            with open(tmp_name, "wb") as w:
                w.write("hello hello".encode("utf-8"))
            b = blobs.Blob.from_python_std(tmp_name)
            assert b.mode == "wb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            assert b.remote_location.startswith(t.name)
            assert b.local_path == tmp_name
            with open(b.remote_location, "rb") as r:
                assert r.read() == "hello hello".encode("utf-8")

    b = blobs.Blob("/tmp/fake")
    b2 = blobs.Blob.from_python_std(b)
    assert b == b2

    with pytest.raises(_user_exceptions.FlyteTypeException):
        blobs.Blob.from_python_std(3)


def test_blob_create_at():
    with test_utils.LocalTestFileSystem() as t:
        with AutoDeletingTempDir("test") as wd:
            tmp_name = wd.get_named_tempfile("tmp")
            b = blobs.Blob.create_at_known_location(tmp_name)
            assert b.local_path is None
            assert b.remote_location == tmp_name
            assert b.mode == "wb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            with b as w:
                w.write("hello hello".encode("utf-8"))

            assert b.local_path.startswith(t.name)
            with open(tmp_name, "rb") as r:
                assert r.read() == "hello hello".encode("utf-8")


def test_blob_fetch_managed():
    with AutoDeletingTempDir("test") as wd:
        with test_utils.LocalTestFileSystem() as t:
            tmp_name = wd.get_named_tempfile("tmp")
            with open(tmp_name, "wb") as w:
                w.write("hello".encode("utf-8"))

            b = blobs.Blob.fetch(tmp_name)
            assert b.local_path.startswith(t.name)
            assert b.remote_location == tmp_name
            assert b.mode == "rb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            with b as r:
                assert r.read() == "hello".encode("utf-8")

            with pytest.raises(_user_exceptions.FlyteAssertion):
                blobs.Blob.fetch(tmp_name, local_path=b.local_path)

            with open(tmp_name, "wb") as w:
                w.write("bye".encode("utf-8"))

            b2 = blobs.Blob.fetch(tmp_name, local_path=b.local_path, overwrite=True)
            with b2 as r:
                assert r.read() == "bye".encode("utf-8")

        with pytest.raises(_user_exceptions.FlyteAssertion):
            blobs.Blob.fetch(tmp_name)


def test_blob_fetch_unmanaged():
    with AutoDeletingTempDir("test") as wd:
        with AutoDeletingTempDir("test2") as t:
            tmp_name = wd.get_named_tempfile("source")
            tmp_sink = t.get_named_tempfile("sink")
            with open(tmp_name, "wb") as w:
                w.write("hello".encode("utf-8"))

            b = blobs.Blob.fetch(tmp_name, local_path=tmp_sink)
            assert b.local_path == tmp_sink
            assert b.remote_location == tmp_name
            assert b.mode == "rb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            with b as r:
                assert r.read() == "hello".encode("utf-8")

            with pytest.raises(_user_exceptions.FlyteAssertion):
                blobs.Blob.fetch(tmp_name, local_path=tmp_sink)

            with open(tmp_name, "wb") as w:
                w.write("bye".encode("utf-8"))

            b2 = blobs.Blob.fetch(tmp_name, local_path=tmp_sink, overwrite=True)
            with b2 as r:
                assert r.read() == "bye".encode("utf-8")


def test_blob_double_enter():
    with test_utils.LocalTestFileSystem():
        with AutoDeletingTempDir("test") as wd:
            b = blobs.Blob(wd.get_named_tempfile("sink"), mode="wb")
            with b:
                with pytest.raises(_user_exceptions.FlyteAssertion):
                    with b:
                        pass


def test_blob_download_managed():
    with AutoDeletingTempDir("test") as wd:
        with test_utils.LocalTestFileSystem() as t:
            tmp_name = wd.get_named_tempfile("tmp")
            with open(tmp_name, "wb") as w:
                w.write("hello".encode("utf-8"))

            b = blobs.Blob(tmp_name)
            b.download()
            assert b.local_path.startswith(t.name)
            assert b.remote_location == tmp_name
            assert b.mode == "rb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            with b as r:
                assert r.read() == "hello".encode("utf-8")

            b2 = blobs.Blob(tmp_name)
            with pytest.raises(_user_exceptions.FlyteAssertion):
                b2.download(b.local_path)

            with open(tmp_name, "wb") as w:
                w.write("bye".encode("utf-8"))

            b2 = blobs.Blob(tmp_name)
            b2.download(local_path=b.local_path, overwrite=True)
            with b2 as r:
                assert r.read() == "bye".encode("utf-8")

        b = blobs.Blob(tmp_name)
        with pytest.raises(_user_exceptions.FlyteAssertion):
            b.download()


def test_blob_download_unmanaged():
    with AutoDeletingTempDir("test") as wd:
        with AutoDeletingTempDir("test2") as t:
            tmp_name = wd.get_named_tempfile("source")
            tmp_sink = t.get_named_tempfile("sink")
            with open(tmp_name, "wb") as w:
                w.write("hello".encode("utf-8"))

            b = blobs.Blob(tmp_name)
            b.download(tmp_sink)
            assert b.local_path == tmp_sink
            assert b.remote_location == tmp_name
            assert b.mode == "rb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            with b as r:
                assert r.read() == "hello".encode("utf-8")

            b = blobs.Blob(tmp_name)
            with pytest.raises(_user_exceptions.FlyteAssertion):
                b.download(tmp_sink)

            with open(tmp_name, "wb") as w:
                w.write("bye".encode("utf-8"))

            b2 = blobs.Blob(tmp_name)
            b2.download(tmp_sink, overwrite=True)
            with b2 as r:
                assert r.read() == "bye".encode("utf-8")


def test_multipart_blob():
    b = blobs.MultiPartBlob("/tmp/fake", mode="w", format="csv")
    assert b.remote_location == "/tmp/fake/"
    assert b.local_path is None
    assert b.mode == "w"
    assert b.metadata.type.format == "csv"
    assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART


def _generate_multipart_blob_data(tmp_dir):
    n = tmp_dir.get_named_tempfile("0")
    with open(n, "wb") as w:
        w.write("part0".encode("utf-8"))
    n = tmp_dir.get_named_tempfile("1")
    with open(n, "wb") as w:
        w.write("part1".encode("utf-8"))
    n = tmp_dir.get_named_tempfile("2")
    with open(n, "wb") as w:
        w.write("part2".encode("utf-8"))


def test_multipart_blob_from_python_std():
    with test_utils.LocalTestFileSystem() as t:
        with AutoDeletingTempDir("test") as wd:
            _generate_multipart_blob_data(wd)
            b = blobs.MultiPartBlob.from_python_std(wd.name)
            assert b.mode == "wb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART
            assert b.remote_location.startswith(t.name)
            assert b.local_path == wd.name
            with open(os.path.join(b.remote_location, "0"), "rb") as r:
                assert r.read() == "part0".encode("utf-8")
            with open(os.path.join(b.remote_location, "1"), "rb") as r:
                assert r.read() == "part1".encode("utf-8")
            with open(os.path.join(b.remote_location, "2"), "rb") as r:
                assert r.read() == "part2".encode("utf-8")

    b = blobs.MultiPartBlob("/tmp/fake/")
    b2 = blobs.MultiPartBlob.from_python_std(b)
    assert b == b2

    with pytest.raises(_user_exceptions.FlyteTypeException):
        blobs.MultiPartBlob.from_python_std(3)


def test_multipart_blob_create_at():
    with test_utils.LocalTestFileSystem():
        with AutoDeletingTempDir("test") as wd:
            b = blobs.MultiPartBlob.create_at_known_location(wd.name)
            assert b.local_path is None
            assert b.remote_location == wd.name + "/"
            assert b.mode == "wb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART
            with b.create_part("0") as w:
                w.write("part0".encode("utf-8"))
            with b.create_part("1") as w:
                w.write("part1".encode("utf-8"))
            with b.create_part("2") as w:
                w.write("part2".encode("utf-8"))

            with open(os.path.join(wd.name, "0"), "rb") as r:
                assert r.read() == "part0".encode("utf-8")
            with open(os.path.join(wd.name, "1"), "rb") as r:
                assert r.read() == "part1".encode("utf-8")
            with open(os.path.join(wd.name, "2"), "rb") as r:
                assert r.read() == "part2".encode("utf-8")


def test_multipart_blob_fetch_managed():
    with AutoDeletingTempDir("test") as wd:
        with test_utils.LocalTestFileSystem() as t:
            _generate_multipart_blob_data(wd)

            b = blobs.MultiPartBlob.fetch(wd.name)
            assert b.local_path.startswith(t.name)
            assert b.remote_location == wd.name + "/"
            assert b.mode == "rb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART
            with b as r:
                assert r[0].read() == "part0".encode("utf-8")
                assert r[1].read() == "part1".encode("utf-8")
                assert r[2].read() == "part2".encode("utf-8")

            with pytest.raises(_user_exceptions.FlyteAssertion):
                blobs.MultiPartBlob.fetch(wd.name, local_path=b.local_path)

            with open(os.path.join(wd.name, "0"), "wb") as w:
                w.write("bye".encode("utf-8"))

            b2 = blobs.MultiPartBlob.fetch(wd.name, local_path=b.local_path, overwrite=True)
            with b2 as r:
                assert r[0].read() == "bye".encode("utf-8")
                assert r[1].read() == "part1".encode("utf-8")
                assert r[2].read() == "part2".encode("utf-8")

        with pytest.raises(_user_exceptions.FlyteAssertion):
            blobs.Blob.fetch(wd.name)


def test_multipart_blob_fetch_unmanaged():
    with AutoDeletingTempDir("test") as wd:
        with AutoDeletingTempDir("test2") as t:
            _generate_multipart_blob_data(wd)
            tmp_sink = t.get_named_tempfile("sink")

            b = blobs.MultiPartBlob.fetch(wd.name, local_path=tmp_sink)
            assert b.local_path == tmp_sink
            assert b.remote_location == wd.name + "/"
            assert b.mode == "rb"
            assert b.metadata.type.format == ""
            assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART
            with b as r:
                assert r[0].read() == "part0".encode("utf-8")
                assert r[1].read() == "part1".encode("utf-8")
                assert r[2].read() == "part2".encode("utf-8")

            with pytest.raises(_user_exceptions.FlyteAssertion):
                blobs.MultiPartBlob.fetch(wd.name, local_path=tmp_sink)

            with open(os.path.join(wd.name, "0"), "wb") as w:
                w.write("bye".encode("utf-8"))

            b2 = blobs.MultiPartBlob.fetch(wd.name, local_path=tmp_sink, overwrite=True)
            with b2 as r:
                assert r[0].read() == "bye".encode("utf-8")
                assert r[1].read() == "part1".encode("utf-8")
                assert r[2].read() == "part2".encode("utf-8")


def test_multipart_blob_no_enter_on_write():
    with test_utils.LocalTestFileSystem():
        b = blobs.MultiPartBlob.create_at_any_location()
        with pytest.raises(_user_exceptions.FlyteAssertion):
            with b:
                pass

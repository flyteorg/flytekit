from flytekit.common.types import blobs
from flytekit.common.types.impl import blobs as blob_impl
from flytekit.models.core import types as _core_types, literals as _literal_models
from flytekit.sdk import test_utils


def test_blob_instantiator():
    b = blobs.BlobInstantiator.create_at_known_location("abc")
    assert isinstance(b, blob_impl.Blob)
    assert b.remote_location == "abc"
    assert b.mode == "wb"
    assert b.metadata.type.format == ""


def test_blob():
    with test_utils.LocalTestFileSystem() as t:
        b = blobs.Blob()
        assert isinstance(b, blob_impl.Blob)
        assert b.remote_location.startswith(t.name)
        assert b.mode == "wb"
        assert b.metadata.type.format == ""

        b2 = blobs.Blob(b)
        assert isinstance(b2, blobs.Blob)
        assert b2.scalar.blob.uri == b.remote_location
        assert b2.scalar.blob.metadata == b.metadata

        b3 = blobs.Blob.from_string("/a/b/c")
        assert isinstance(b3, blobs.Blob)
        assert b3.scalar.blob.uri == "/a/b/c"
        assert b3.scalar.blob.metadata.type.format == ""


def test_blob_promote_from_model():
    m = _literal_models.Literal(
        scalar=_literal_models.Scalar(
            blob=_literal_models.Blob(
                _literal_models.BlobMetadata(
                    _core_types.BlobType(
                        format="f",
                        dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
                    )
                ),
                "some/path",
            )
        )
    )
    b = blobs.Blob.promote_from_model(m)
    assert b.value.blob.uri == "some/path"
    assert b.value.blob.metadata.type.format == "f"
    assert b.value.blob.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE


def test_blob_to_python_std():
    impl = blob_impl.Blob("some/path", format="something")
    b = blobs.Blob(impl).to_python_std()
    assert b.metadata.type.format == "something"
    assert b.metadata.type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
    assert b.uri == "some/path"


def test_csv_instantiator():
    b = blobs.CsvInstantiator.create_at_known_location("abc")
    assert isinstance(b, blob_impl.Blob)
    assert b.remote_location == "abc"
    assert b.mode == "w"
    assert b.metadata.type.format == "csv"


def test_csv():
    with test_utils.LocalTestFileSystem() as t:
        b = blobs.CSV()
        assert isinstance(b, blob_impl.Blob)
        assert b.remote_location.startswith(t.name)
        assert b.mode == "w"
        assert b.metadata.type.format == "csv"

        b2 = blobs.CSV(b)
        assert isinstance(b2, blobs.Blob)
        assert b2.scalar.blob.uri == b.remote_location
        assert b2.scalar.blob.metadata == b.metadata

        b3 = blobs.CSV.from_string("/a/b/c")
        assert isinstance(b3, blobs.Blob)
        assert b3.scalar.blob.uri == "/a/b/c"
        assert b3.scalar.blob.metadata.type.format == "csv"


def test_multipartblob_instantiator():
    b = blobs.MultiPartBlob.create_at_known_location("abc")
    assert isinstance(b, blob_impl.MultiPartBlob)
    assert b.remote_location == "abc" + "/"
    assert b.mode == "wb"
    assert b.metadata.type.format == ""


def test_multipartblob():
    with test_utils.LocalTestFileSystem() as t:
        b = blobs.MultiPartBlob()
        assert isinstance(b, blob_impl.MultiPartBlob)
        assert b.remote_location.startswith(t.name)
        assert b.mode == "wb"
        assert b.metadata.type.format == ""

        b2 = blobs.MultiPartBlob(b)
        assert isinstance(b2, blobs.MultiPartBlob)
        assert b2.scalar.blob.uri == b.remote_location
        assert b2.scalar.blob.metadata == b.metadata

        b3 = blobs.MultiPartBlob.from_string("/a/b/c")
        assert isinstance(b3, blobs.MultiPartBlob)
        assert b3.scalar.blob.uri == "/a/b/c/"
        assert b3.scalar.blob.metadata.type.format == ""


def test_multipartcsv_instantiator():
    b = blobs.MultiPartCsvInstantiator.create_at_known_location("abc")
    assert isinstance(b, blob_impl.MultiPartBlob)
    assert b.remote_location == "abc" + "/"
    assert b.mode == "w"
    assert b.metadata.type.format == "csv"


def test_multipartcsv():
    with test_utils.LocalTestFileSystem() as t:
        b = blobs.MultiPartCSV()
        assert isinstance(b, blob_impl.MultiPartBlob)
        assert b.remote_location.startswith(t.name)
        assert b.mode == "w"
        assert b.metadata.type.format == "csv"

        b2 = blobs.MultiPartCSV(b)
        assert isinstance(b2, blobs.MultiPartCSV)
        assert b2.scalar.blob.uri == b.remote_location
        assert b2.scalar.blob.metadata == b.metadata

        b3 = blobs.MultiPartCSV.from_string("/a/b/c")
        assert isinstance(b3, blobs.MultiPartCSV)
        assert b3.scalar.blob.uri == "/a/b/c/"
        assert b3.scalar.blob.metadata.type.format == "csv"

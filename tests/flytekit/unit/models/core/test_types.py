from __future__ import absolute_import
from flytekit.models.core import types as _types
from flyteidl.core import types_pb2 as _types_pb2


def test_blob_dimensionality():
    assert _types.BlobType.BlobDimensionality.SINGLE == _types_pb2.BlobType.SINGLE
    assert _types.BlobType.BlobDimensionality.MULTIPART == _types_pb2.BlobType.MULTIPART


def test_blob_type():
    o = _types.BlobType(
        format="csv",
        dimensionality=_types.BlobType.BlobDimensionality.SINGLE,
    )
    assert o.format == "csv"
    assert o.dimensionality == _types.BlobType.BlobDimensionality.SINGLE

    o2 = _types.BlobType.from_flyte_idl(o.to_flyte_idl())
    assert o == o2
    assert o2.format == "csv"
    assert o2.dimensionality == _types.BlobType.BlobDimensionality.SINGLE

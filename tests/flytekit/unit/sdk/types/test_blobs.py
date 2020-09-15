import pytest

from flytekit.common.types.impl import blobs as _blob_impl
from flytekit.sdk import types as _sdk_types


@pytest.mark.parametrize(
    "blob_tuple",
    [
        (_sdk_types.Types.Blob, _blob_impl.Blob),
        (_sdk_types.Types.CSV, _blob_impl.Blob),
        (_sdk_types.Types.MultiPartBlob, _blob_impl.MultiPartBlob),
        (_sdk_types.Types.MultiPartCSV, _blob_impl.MultiPartBlob),
    ],
)
def test_instantiable_blobs(blob_tuple):
    sdk_type, impl = blob_tuple

    blob_inst = sdk_type()
    blob_type_inst = sdk_type(blob_inst)
    assert isinstance(blob_inst, impl)
    assert isinstance(blob_type_inst, sdk_type)

    with pytest.raises(Exception):
        sdk_type(1, 2)

    with pytest.raises(Exception):
        sdk_type(a=1)

    blob_inst = sdk_type.create_at_known_location("abc")
    assert isinstance(blob_inst, impl)

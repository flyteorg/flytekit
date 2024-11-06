import pytest
from flyteidl.core.literals_pb2 import Blob, BlobMetadata, Literal, LiteralCollection, Scalar
from flyteidl.core.types_pb2 import BlobType

from flytekit.utils.pbhash import compute_hash_string


@pytest.mark.parametrize(
    "lit, expected_hash",
    [
        (
            Literal(
                scalar=Scalar(
                    blob=Blob(
                        uri="s3://my-bucket/my-key",
                        metadata=BlobMetadata(
                            type=BlobType(
                                format="PythonPickle",
                                dimensionality=BlobType.BlobDimensionality.SINGLE,
                            ),
                        ),
                    ),
                ),
            ),
            "KdNNbLBYoamXYLz8SBuJd/kVDPxO4gVGdNQl61qeTfA=",
        ),
        (
            Literal(
                scalar=Scalar(
                    blob=Blob(
                        metadata=BlobMetadata(
                            type=BlobType(
                                format="PythonPickle",
                                dimensionality=BlobType.BlobDimensionality.SINGLE,
                            ),
                        ),
                        uri="s3://my-bucket/my-key",
                    ),
                ),
            ),
            "KdNNbLBYoamXYLz8SBuJd/kVDPxO4gVGdNQl61qeTfA=",
        ),
        (
            # Literal collection
            Literal(
                collection=LiteralCollection(
                    literals=[
                        Literal(
                            scalar=Scalar(
                                blob=Blob(
                                    metadata=BlobMetadata(
                                        type=BlobType(
                                            format="PythonPickle",
                                            dimensionality=BlobType.BlobDimensionality.SINGLE,
                                        ),
                                    ),
                                    uri="s3://my-bucket/my-key",
                                ),
                            ),
                        ),
                    ],
                ),
            ),
            "RauoCNnZfCSHgcmMKVugozLAcssq/mWdMjbGanRJufI=",
        )
    ],
)
def test_lit(lit, expected_hash):
    assert compute_hash_string(lit) == expected_hash

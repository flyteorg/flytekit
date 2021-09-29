from flytekit.core import context_manager
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
from flytekit.models.types import LiteralType
from flytekit.types.pickle.pickle import FlytePickle, FlytePickleTransformer


def test_to_python_value_and_literal():
    ctx = context_manager.FlyteContext.current_context()
    tf = FlytePickleTransformer()
    python_val = "fake_output"
    lt = tf.get_literal_type(FlytePickle)

    lv = tf.to_literal(ctx, python_val, type(python_val), lt)  # type: ignore
    assert lv.scalar.blob.metadata == BlobMetadata(
        type=BlobType(
            format=FlytePickleTransformer.PYTHON_PICKLE_FORMAT,
            dimensionality=BlobType.BlobDimensionality.SINGLE,
        )
    )
    assert lv.scalar.blob.uri is not None

    output = tf.to_python_value(ctx, lv, str)
    assert output == python_val


def test_get_literal_type():
    tf = FlytePickleTransformer()
    lt = tf.get_literal_type(FlytePickle)
    assert lt == LiteralType(
        blob=BlobType(
            format=FlytePickleTransformer.PYTHON_PICKLE_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE
        )
    )

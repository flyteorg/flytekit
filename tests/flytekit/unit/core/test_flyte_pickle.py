from collections import OrderedDict
from typing import List

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.task import task
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
from flytekit.models.types import LiteralType
from flytekit.types.pickle.pickle import FlytePickle, FlytePickleTransformer

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = context_manager.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


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


def test_nested():
    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    @task
    def t1(a: int) -> List[List[Foo]]:
        return [[Foo(number=a)]]

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    print(task_spec)

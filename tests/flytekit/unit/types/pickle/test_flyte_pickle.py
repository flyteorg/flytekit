import sys
from collections import OrderedDict
from collections.abc import Sequence
from typing import Any, Dict, List, Union, Tuple

import numpy as np
import pytest
from typing_extensions import Annotated

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.models.core.types import BlobType
from flytekit.models.literals import BlobMetadata
from flytekit.models.types import LiteralType
from flytekit.tools.translator import get_serializable
from flytekit.types.pickle.pickle import FlytePickle, FlytePickleTransformer

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
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
    expected_lt = LiteralType(
        blob=BlobType(
            format=FlytePickleTransformer.PYTHON_PICKLE_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE
        )
    )
    expected_lt.metadata = {"python_class_name": str(FlytePickle)}
    assert lt == expected_lt


def test_nested():
    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    @task
    def t1(a: int) -> List[List[Foo]]:
        return [[Foo(number=a)]]

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert (
        task_spec.template.interface.outputs["o0"].type.collection_type.collection_type.blob.format
        is FlytePickleTransformer.PYTHON_PICKLE_FORMAT
    )


def test_nested2():
    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    @task
    def t1(a: int) -> List[Dict[str, Foo]]:
        return [{"a": Foo(number=a)}]

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert (
        task_spec.template.interface.outputs["o0"].type.collection_type.map_value_type.blob.format
        is FlytePickleTransformer.PYTHON_PICKLE_FORMAT
    )


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
def test_union():
    import pandas as pd

    @task
    def t1(data: Annotated[Union[np.ndarray, pd.DataFrame, Sequence], "some annotation"]):
        print(data)

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    variants = task_spec.template.interface.inputs["data"].type.union_type.variants
    assert variants[0].blob.format == "NumpyArray"
    assert variants[1].structured_dataset_type.format == ""
    assert variants[2].blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT


def test_artf():
    from flytekit.core.artifact import Artifact

    a1 = Artifact(name="my_a1", partition_keys=["a"])

    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    @task
    def t1(a: int) -> Annotated[Foo, a1(a="bar")]:
        return Foo(number=a)

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    md = task_spec.template.interface.outputs["o0"].type.metadata["python_class_name"]
    assert "0x" not in str(md)


def test_default_args_task():
    default_val = 123
    input_val = "foo"

    @task
    def t1(a: Any = default_val) -> Any:
        return a

    @workflow
    def wf_no_input() -> Any:
        return t1()

    @workflow
    def wf_with_input() -> Any:
        return t1(a=input_val)

    @workflow
    def wf_with_sub_wf() -> Tuple[Any, Any]:
        return (wf_no_input(), wf_with_input())

    wf_no_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    metadata = BlobMetadata(
        type=BlobType(
            format="PythonPickle",
            dimensionality=BlobType.BlobDimensionality.SINGLE,
        ),
    )
    assert wf_no_input_spec.template.nodes[0].inputs[0].binding.value.blob.metadata == metadata
    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value.blob.metadata == metadata

    output_type = LiteralType(
        blob=BlobType(
            format="PythonPickle",
            dimensionality=BlobType.BlobDimensionality.SINGLE,
        ),
        metadata={
            "python_class_name": "typing.Any",
        },
    )
    assert wf_no_input_spec.template.interface.outputs["o0"].type == output_type
    assert wf_with_input_spec.template.interface.outputs["o0"].type == output_type

    assert wf_no_input() == default_val
    assert wf_with_input() == input_val
    assert wf_with_sub_wf() == (default_val, input_val)

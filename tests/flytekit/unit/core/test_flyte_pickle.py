from collections import OrderedDict
from collections.abc import Sequence
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from typing_extensions import Annotated

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.task import task
from flytekit.core.type_engine import FlytePickleTransformer
from flytekit.tools.translator import get_serializable

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


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


def test_union():
    @task
    def t1(data: Annotated[Union[np.ndarray, pd.DataFrame, Sequence], "some annotation"]):
        print(data)

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    variants = task_spec.template.interface.inputs["data"].type.union_type.variants
    assert variants[0].blob.format == "NumpyArray"
    assert variants[1].structured_dataset_type.format == ""
    assert variants[2].blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT

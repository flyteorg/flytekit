import sys
from dataclasses import dataclass
from typing import List, Optional

import pytest
from dataclasses_json import DataClassJsonMixin
from mashumaro.mixins.json import DataClassJSONMixin
from typing_extensions import Annotated

from flytekit.core.task import task
from flytekit.core.type_engine import DataclassTransformer
from flytekit.core.workflow import workflow

from dataclasses import dataclass, field
import base64
import pickle
from flytekit.core.resources import Resources
from flytekit.core.dynamic_workflow_task import dynamic
from mashumaro.config import BaseConfig


class SimpleObjectOriginal:
    def __init__(self, a: Optional[str] = None, b: Optional[int] = None):
        self.a = a
        self.b = b


def encode_object(obj: SimpleObjectOriginal) -> str:
    s = base64.b64encode(pickle.dumps(obj)).decode("utf-8")
    print(f"Encode object to {s}")
    return s


def decode_object(object_value: str) -> SimpleObjectOriginal:
    print(f"Decoding from string {object_value}")
    return pickle.loads(base64.b64decode(object_value.encode("utf-8")))


@dataclass
class SimpleObjectOriginalMixin(DataClassJsonMixin):
    # This is a mixin that adds a SimpleObjectOriginal field to any dataclass.

    simple_object: SimpleObjectOriginal = field(
        default_factory=SimpleObjectOriginal,
    )

    class Config(BaseConfig):
        serialization_strategy = {
            SimpleObjectOriginal: {
                # you can use specific str values for datetime here as well
                "deserialize": decode_object,
                "serialize": encode_object,
            },
        }


@dataclass
class ParentDC(SimpleObjectOriginalMixin):
    parent_val: str = ""


@task
def generate_result() -> SimpleObjectOriginalMixin:
    return SimpleObjectOriginalMixin(simple_object=SimpleObjectOriginal(a="a", b=1))


@task
def check_result(obj: SimpleObjectOriginalMixin):
    assert obj.simple_object is not None

@task
def generate_int() -> int:
    return 42


def test_simple_object_yee():
    @dynamic
    def my_dynamic():
        result = generate_result()
        n = check_result(obj=result)
        n.with_overrides(limits=Resources(cpu="3", mem="500Mi"))

    my_dynamic()

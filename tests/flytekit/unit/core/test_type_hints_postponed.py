from __future__ import annotations

import typing
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.task import task

serialization_settings = context_manager.SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)


@dataclass_json
@dataclass
class Foo(object):
    x: int
    y: str
    z: typing.Dict[str, str]


@dataclass_json
@dataclass
class Bar(object):
    x: int
    y: str
    z: Foo


@task
def t1() -> Foo:
    return Foo(x=1, y="foo", z={"hello": "world"})


def test_guess_dict4():
    print(t1.interface.outputs["o0"])
    assert t1.interface.outputs["o0"].type.metadata["definitions"]["FooSchema"] is not None

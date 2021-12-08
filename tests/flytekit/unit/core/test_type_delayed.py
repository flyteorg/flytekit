from __future__ import annotations

import typing
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from flytekit.core.type_engine import TypeEngine


@dataclass_json
@dataclass
class Foo(object):
    x: int
    y: str
    z: typing.Dict[str, str]


def test_jsondc_schemaize():
    try:
        TypeEngine.to_literal_type(Foo)
    except Exception as e:
        assert "unsupported field type" in f"{e}"

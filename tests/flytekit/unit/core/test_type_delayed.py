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
    lt = TypeEngine.to_literal_type(Foo)
    pt = TypeEngine.guess_python_type(lt)

    # When postponed annotations are enabled, dataclass_json will not work and we'll end up with a
    # schemaless generic.
    # This test basically tests the broken behavior. Remove this test if
    # https://github.com/lovasoa/marshmallow_dataclass/issues/13 is ever fixed.
    assert pt is dict

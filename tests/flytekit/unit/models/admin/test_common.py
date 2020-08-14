from __future__ import absolute_import

from flytekit.models.admin import common as _common
import pytest as _pytest


def test_sort():
    o = _common.Sort(key="abc", direction=_common.Sort.Direction.ASCENDING)
    assert o.key == "abc"
    assert o.direction == _common.Sort.Direction.ASCENDING

    o2 = _common.Sort.from_flyte_idl(o.to_flyte_idl())
    assert o2 == o
    assert o2.key == "abc"
    assert o2.direction == _common.Sort.Direction.ASCENDING


def test_sort_parse():
    o = _common.Sort.from_python_std(' asc(my"\wackyk3y) ')
    assert o.key == 'my"\wackyk3y'
    assert o.direction == _common.Sort.Direction.ASCENDING

    o = _common.Sort.from_python_std("  desc(   mykey   ) ")
    assert o.key == "mykey"
    assert o.direction == _common.Sort.Direction.DESCENDING

    with _pytest.raises(ValueError):
        _common.Sort.from_python_std("asc(abc")

    with _pytest.raises(ValueError):
        _common.Sort.from_python_std("asce(abc)")

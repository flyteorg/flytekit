import pytest

from flytekit.legacy.engines import loader
from flytekit.legacy.engines.unit import engine as _unit_engine


def test_unit_load():
    assert isinstance(loader.get_engine("unit"), _unit_engine.UnitTestEngineFactory)


def test_bad_load():
    with pytest.raises(Exception):
        loader.get_engine("badname")

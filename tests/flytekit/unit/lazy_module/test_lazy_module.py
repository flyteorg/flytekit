import pytest

from flytekit.lazy_import.lazy_module import DummyModule, lazy_module


def test_lazy_module():
    mod = lazy_module("pandas")
    assert mod.__name__ == "pandas"
    mod = lazy_module("fake_module")
    assert isinstance(mod, DummyModule)
    with pytest.raises(ImportError, match="Module fake_module is not yet installed."):
        print(mod.attr)

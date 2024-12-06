import pytest

from flytekit.lazy_import.lazy_module import LazyModule, lazy_module, is_imported


def test_lazy_module():
    mod = lazy_module("click")
    assert mod.__name__ == "click"
    mod = lazy_module("fake_module")
    assert not is_imported("fake_module")
    assert isinstance(mod, LazyModule)
    with pytest.raises(ImportError, match="Module fake_module is not yet installed."):
        print(mod.attr)

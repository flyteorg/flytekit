import sys
from unittest.mock import Mock

import pytest
from flytekit.lazy_import.lazy_module import _LazyModule, lazy_module, is_imported


def test_lazy_module():
    mod = lazy_module("click")
    assert mod.__name__ == "click"
    mod = lazy_module("fake_module")

    sys.modules["fake_module"] = mod
    assert not is_imported("fake_module")
    assert isinstance(mod, _LazyModule)
    with pytest.raises(ImportError, match="Module fake_module is not yet installed."):
        print(mod.attr)

    non_lazy_module = Mock()
    non_lazy_module.__name__ = 'NonLazyModule'
    sys.modules["fake_module"] = non_lazy_module
    assert is_imported("fake_module")

    assert is_imported("dataclasses")


def test_lazy_module_submodule_already_imported_by_parent(tmp_path, monkeypatch):
    # A parent package whose __init__ imports the submodule that is requested lazily.
    pkg = tmp_path / "lazy_parent_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("from . import sub\n")
    (pkg / "sub.py").write_text("class Marker:\n    pass\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    names = ("lazy_parent_pkg", "lazy_parent_pkg.sub")
    for name in names:
        sys.modules.pop(name, None)

    try:
        lazy_sub = lazy_module("lazy_parent_pkg.sub")

        import lazy_parent_pkg.sub as real_sub

        # the lazily requested module is the one the parent already imported, not a second copy
        assert lazy_sub is real_sub
        assert lazy_sub.Marker is sys.modules["lazy_parent_pkg"].sub.Marker
    finally:
        for name in names:
            sys.modules.pop(name, None)

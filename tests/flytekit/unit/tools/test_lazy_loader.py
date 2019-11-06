from __future__ import absolute_import
from flytekit.tools import lazy_loader
import pytest
import six


def test_lazy_loader_error_message():
    lazy_mod = lazy_loader.lazy_load_module("made.up.module")
    lazy_loader.LazyLoadPlugin(
        "uninstalled_plugin",
        [],
        [lazy_mod]
    )
    with pytest.raises(ImportError) as e:
        lazy_mod.some_bad_attr

    assert 'uninstalled_plugin' in six.text_type(e.value)
    assert 'flytekit[all]' in six.text_type(e.value)

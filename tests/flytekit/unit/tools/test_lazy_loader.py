from __future__ import absolute_import
from flytekit.tools import lazy_loader
import pytest
import six


def test_lazy_loader_error_message():
    lazy_loader.LazyLoadPlugin(
        "uninstalled_plugin",
        [],
        ["made.up.module"]
    )
    with pytest.raises(ImportError) as e:
        import made.up.module as m
        m.a

    assert 'flytekit[uninstalled_plugin]' in six.text_type(e)

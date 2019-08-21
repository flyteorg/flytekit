from __future__ import absolute_import
from flytekit import configuration as _config
from flytekit.clis.sdk_in_container import constants as _constants
import mock as _mock
import pytest
import os
import sys


def _fake_module_load(name):
    assert name == 'common.workflows'
    from common.workflows import simple
    yield simple


@pytest.yield_fixture(scope='function', autouse=True)
def mock_ctx():
    with _config.TemporaryConfiguration(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../common/configs/local.config')
    ):
        sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../..'))
        try:
            with _mock.patch('flytekit.tools.module_loader.iterate_modules') as mock_module_load:
                mock_module_load.side_effect = _fake_module_load
                ctx = _mock.MagicMock()
                ctx.obj = {
                    _constants.CTX_PACKAGES: 'common.workflows',
                    _constants.CTX_PROJECT: 'tests',
                    _constants.CTX_DOMAIN: 'unit',
                    _constants.CTX_VERSION: 'version'
                }
                yield ctx
        finally:
            sys.path.pop()

from __future__ import absolute_import
from flytekit.configuration import set_flyte_config_file as _set_flyte_config_file, \
    common as _common, \
    TemporaryConfiguration as _TemporaryConfiguration
import os as _os


def test_configuration_file():
    with _TemporaryConfiguration(
            _os.path.join(_os.path.dirname(_os.path.realpath(__file__)), 'configs/good.config')):
        assert _common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
            'this.module,that.module'
    assert _common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') is None


def test_internal_overrides():
    with _TemporaryConfiguration(
            _os.path.join(_os.path.dirname(_os.path.realpath(__file__)), 'configs/good.config'),
            {'foo': 'bar'}):
        assert _os.environ.get('FLYTE_INTERNAL_FOO') == 'bar'
    assert _os.environ.get('FLYTE_INTERNAL_FOO') is None


def test_no_configuration_file():
    _set_flyte_config_file(_os.path.join(_os.path.dirname(_os.path.realpath(__file__)), 'configs/good.config'))
    with _TemporaryConfiguration(None):
        assert _common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') is None
    assert _common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
        'this.module,that.module'


def test_nonexist_configuration_file():
    _set_flyte_config_file(_os.path.join(_os.path.dirname(_os.path.realpath(__file__)), 'configs/good.config'))
    with _TemporaryConfiguration('/foo/bar'):
        assert _common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') is None
    assert _common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
        'this.module,that.module'

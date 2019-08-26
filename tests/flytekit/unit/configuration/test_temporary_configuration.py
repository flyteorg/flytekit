from __future__ import absolute_import
from flytekit.configuration import set_flyte_config_file, common, TemporaryConfiguration
import os


def test_configuration_file():
    with TemporaryConfiguration(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/good.config')):
        assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
            'this.module,that.module'
    assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') is None


def test_internal_overrides():
    with TemporaryConfiguration(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/good.config'),
            {'foo': 'bar'}):
        assert os.environ.get('FLYTE_INTERNAL_FOO') == 'bar'
    assert os.environ.get('FLYTE_INTERNAL_FOO') is None


def test_no_configuration_file():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/good.config'))
    with TemporaryConfiguration(None):
        assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') is None
    assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
        'this.module,that.module'


def test_nonexist_configuration_file():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/good.config'))
    with TemporaryConfiguration('/foo/bar'):
        assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') is None
    assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
        'this.module,that.module'

from __future__ import absolute_import
from flytekit.configuration import set_flyte_config_file, common
import os
import pytest


def test_file_loader_bad():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/bad.config'))
    with pytest.raises(Exception):
        common.CONFIGURATION_SINGLETON.get_string('a', 'b')


def test_file_loader_good():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/good.config'))
    assert common.CONFIGURATION_SINGLETON.get_string('sdk', 'workflow_packages') == \
        'this.module,that.module'
    assert common.CONFIGURATION_SINGLETON.get_string('auth', 'assumable_iam_role') == 'some_role'


def test_env_var_precedence_string():
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs/good.config'))

    assert common.FlyteIntegerConfigurationEntry('madeup', 'int_value').get() == 3
    assert common.FlyteStringConfigurationEntry('madeup', 'string_value').get() == 'abc'

    old_environ = dict(os.environ)
    try:
        os.environ['FLYTE_MADEUP_INT_VALUE'] = '10'
        os.environ["FLYTE_MADEUP_STRING_VALUE"] = 'overridden'
        assert common.FlyteIntegerConfigurationEntry('madeup', 'int_value').get() == 10
        assert common.FlyteStringConfigurationEntry('madeup', 'string_value').get() == 'overridden'
    finally:
        os.environ.clear()
        os.environ.update(old_environ)

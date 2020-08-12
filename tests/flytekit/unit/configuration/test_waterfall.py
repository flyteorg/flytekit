from __future__ import absolute_import

import os as _os

from flytekit.common.utils import AutoDeletingTempDir as _AutoDeletingTempDir
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.configuration import common as _common
from flytekit.configuration import set_flyte_config_file as _set_flyte_config_file


def test_lookup_waterfall_raw_env_var():
    x = _common.FlyteStringConfigurationEntry("test", "setting", default=None)

    if "FLYTE_TEST_SETTING" in _os.environ:
        del _os.environ["FLYTE_TEST_SETTING"]
    assert x.get() is None

    _os.environ["FLYTE_TEST_SETTING"] = "lorem"
    assert x.get() == "lorem"


def test_lookup_waterfall_referenced_env_var():
    x = _common.FlyteStringConfigurationEntry("test", "setting", default=None)

    if "FLYTE_TEST_SETTING" in _os.environ:
        del _os.environ["FLYTE_TEST_SETTING"]
    assert x.get() is None

    if "TEMP_PLACEHOLDER" in _os.environ:
        del _os.environ["TEMP_PLACEHOLDER"]
    _os.environ["TEMP_PLACEHOLDER"] = "lorem"
    _os.environ["FLYTE_TEST_SETTING_FROM_ENV_VAR"] = "TEMP_PLACEHOLDER"
    assert x.get() == "lorem"


def test_lookup_waterfall_referenced_file():
    x = _common.FlyteStringConfigurationEntry("test", "setting", default=None)

    if "FLYTE_TEST_SETTING" in _os.environ:
        del _os.environ["FLYTE_TEST_SETTING"]
    assert x.get() is None

    with _AutoDeletingTempDir("config_testing") as tmp_dir:
        with open(tmp_dir.get_named_tempfile("name"), "w") as fh:
            fh.write("secret_password")

        _os.environ["FLYTE_TEST_SETTING_FROM_FILE"] = tmp_dir.get_named_tempfile("name")
        assert x.get() == "secret_password"

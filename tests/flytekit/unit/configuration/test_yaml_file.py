import os

from flytekit.configuration import ConfigEntry, get_config_file
from flytekit.configuration.file import LegacyConfigEntry, YamlConfigEntry
from flytekit.configuration.internal import Platform


def test_config_entry_file():
    # Pytest feature
    c = ConfigEntry(LegacyConfigEntry("platform", "url", str), YamlConfigEntry("admin.endpoint"), lambda x: x.replace("dns:///", ""))
    assert c.read() is None

    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/sample.yaml"))
    assert c.read(cfg) == "flyte.mycorp.io"

    c = ConfigEntry(LegacyConfigEntry("platform", "url2", str))  # Does not exist
    assert c.read(cfg) is None


def test_config_entry_precedence():
    config_file = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/sample.yaml"))
    res = Platform.INSECURE.read(config_file)
    assert res

    res = Platform.URL.read(config_file)
    assert res == "flyte.mycorp.io"

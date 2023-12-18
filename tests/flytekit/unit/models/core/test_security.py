from unittest.mock import Mock, patch

from flytekit.models.security import Secret


def test_secret():
    obj = Secret("grp", "key")
    obj2 = Secret.from_flyte_idl(obj.to_flyte_idl())
    assert obj2.key == "key"
    assert obj2.group_version is None

    obj = Secret("grp", group_version="v1")
    obj2 = Secret.from_flyte_idl(obj.to_flyte_idl())
    assert obj2.key is None
    assert obj2.group_version == "v1"


@patch("flytekit.models.security.get_plugin")
def test_secret_no_group(get_plugin_mock):
    plugin_mock = Mock()
    plugin_mock.secret_requires_group.return_value = False
    get_plugin_mock.return_value = plugin_mock

    s = Secret(key="key")
    assert s.group is None

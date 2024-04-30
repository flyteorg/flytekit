from unittest.mock import Mock

import pytest

import flytekit.configuration.plugin
from flytekit.core.context_manager import ExecutionState
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


def test_secret_error(monkeypatch):
    # Mock configuration to require groups for this test
    plugin_mock = Mock()
    plugin_mock.secret_requires_group.return_value = True
    mock_global_plugin = {"plugin": plugin_mock}
    monkeypatch.setattr(flytekit.configuration.plugin, "_GLOBAL_CONFIG", mock_global_plugin)

    with pytest.raises(ValueError, match="Group is a required parameter"):
        Secret(key="my_key")


def test_secret_no_group(monkeypatch):
    plugin_mock = Mock()
    plugin_mock.secret_requires_group.return_value = False
    mock_global_plugin = {"plugin": plugin_mock}
    monkeypatch.setattr(flytekit.configuration.plugin, "_GLOBAL_CONFIG", mock_global_plugin)

    s = Secret(key="key")
    assert s.group is None


@pytest.mark.parametrize("execution_mode", list(ExecutionState.Mode))
def test_security_execution_context(monkeypatch, execution_mode, tmpdir):
    # Check that groups in Secrets during any execution state
    context_manager = Mock()
    context = Mock()
    context_manager.current_context.return_value = context
    context.execution_state = ExecutionState(working_dir=tmpdir, mode=execution_mode)

    monkeypatch.setattr(flytekit.core.context_manager, "FlyteContextManager", context_manager)
    s = Secret(key="key")
    assert s.group is None

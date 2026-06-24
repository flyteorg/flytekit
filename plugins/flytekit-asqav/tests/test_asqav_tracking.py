from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from flytekitplugins.asqav import asqav_audit
from flytekitplugins.asqav.tracking import (
    ASQAV_AGENT_NAME_KEY,
    ASQAV_COMPLIANCE_MODE_KEY,
    ASQAV_LINK_TYPE_KEY,
    ASQAV_RECEIPT_TYPE_KEY,
    ASQAV_RISK_CLASS_KEY,
    ASQAV_SCOPE_KEY,
    ASQAV_DEFAULT_RECEIPT_TYPE,
)

from flytekit import Secret, task

secret = Secret(key="asqav-api-key", group="asqav")


def _make_signature_response(signature_id: str, **overrides) -> Mock:
    """Create a mock SignatureResponse with the expected attributes."""
    defaults = {
        "signature_id": signature_id,
        "verification_url": f"https://asqav.com/verify/{signature_id}",
        "algorithm": "ml-dsa-65",
        "compliance_mode": True,
        "receipt_type": ASQAV_DEFAULT_RECEIPT_TYPE,
        "risk_class": None,
    }
    resp = Mock(**{**defaults, **overrides})
    return resp


def test_agent_name_required():
    with pytest.raises(TypeError, match="agent_name"):
        asqav_audit()

    with pytest.raises(ValueError, match="agent_name must be set"):
        asqav_audit(agent_name="")


def test_get_extra_config():
    decorator = asqav_audit(
        agent_name="model-trainer",
        secret=secret,
        receipt_type="protectmcp:lifecycle",
        risk_class="medium",
    )
    config = decorator.get_extra_config()
    assert config[ASQAV_LINK_TYPE_KEY] == "asqav-audit"
    assert config[ASQAV_AGENT_NAME_KEY] == "model-trainer"
    assert config[ASQAV_SCOPE_KEY] == "flyte-task-lifecycle"
    assert config[ASQAV_RECEIPT_TYPE_KEY] == "protectmcp:lifecycle"
    assert config[ASQAV_COMPLIANCE_MODE_KEY] == "True"
    assert config[ASQAV_RISK_CLASS_KEY] == "medium"


def test_get_extra_config_defaults():
    decorator = asqav_audit(agent_name="trainer", secret=secret)
    config = decorator.get_extra_config()
    assert config[ASQAV_LINK_TYPE_KEY] == "asqav-audit"
    assert config[ASQAV_AGENT_NAME_KEY] == "trainer"
    assert config[ASQAV_SCOPE_KEY] == "flyte-task-lifecycle"
    assert config[ASQAV_RECEIPT_TYPE_KEY] == ASQAV_DEFAULT_RECEIPT_TYPE
    assert config[ASQAV_COMPLIANCE_MODE_KEY] == "True"
    assert ASQAV_RISK_CLASS_KEY not in config


@task
@asqav_audit(agent_name="model-trainer", secret=secret)
def train_model() -> str:
    return "done"


@patch("flytekitplugins.asqav.tracking.asqav")
@patch("flytekitplugins.asqav.tracking.Deck")
def test_local_execution(deck_mock, asqav_mock, monkeypatch):
    """Verify the decorator signs started + finished receipts on success."""
    monkeypatch.setenv("ASQAV_API_KEY", "local-test-key")
    agent_mock = MagicMock()
    agent_mock.sign.return_value = _make_signature_response("receipt-started")
    asqav_mock.Agent.create.return_value = agent_mock

    result = train_model()

    assert result == "done"
    asqav_mock.init.assert_called_once()
    asqav_mock.Agent.create.assert_called_once_with(name="model-trainer")
    assert agent_mock.sign.call_count == 2  # started + finished
    deck_mock.assert_called_once()


@patch("flytekitplugins.asqav.tracking.asqav")
@patch("flytekitplugins.asqav.tracking.Deck")
def test_failure_signs_failed_receipt(deck_mock, asqav_mock, monkeypatch):
    """Verify the decorator signs a failed receipt when the task raises."""
    monkeypatch.setenv("ASQAV_API_KEY", "local-test-key")

    @task
    @asqav_audit(agent_name="model-trainer", secret=secret)
    def failing_task():
        raise RuntimeError("model training failed")

    agent_mock = MagicMock()
    agent_mock.sign.return_value = _make_signature_response("receipt-failed")
    asqav_mock.Agent.create.return_value = agent_mock

    with pytest.raises(RuntimeError, match="model training failed"):
        failing_task()

    assert agent_mock.sign.call_count == 2  # started + failed
    deck_mock.assert_called_once()


@patch("flytekitplugins.asqav.tracking.asqav")
@patch("flytekitplugins.asqav.tracking.FlyteContextManager")
@patch("flytekitplugins.asqav.tracking.Deck")
def test_remote_execution_secret_resolution(deck_mock, manager_mock, asqav_mock):
    """Verify secrets are resolved in remote execution."""
    ctx_mock = Mock()
    ctx_mock.execution_state.is_local_execution.return_value = False
    ctx_mock.user_space_params.secrets.get.return_value = "asqav-key-123"
    ctx_mock.user_space_params.execution_id = "exec-001"
    manager_mock.current_context.return_value = ctx_mock

    agent_mock = MagicMock()
    agent_mock.sign.return_value = _make_signature_response("receipt-remote")
    asqav_mock.Agent.create.return_value = agent_mock

    result = train_model()

    assert result == "done"
    ctx_mock.user_space_params.secrets.get.assert_called_with(key="asqav-api-key", group="asqav")
    asqav_mock.init.assert_called_with(api_key="asqav-key-123")
    assert agent_mock.sign.call_count == 2


def get_my_key():
    return "callable-key-456"


@task
@asqav_audit(agent_name="callable-agent", secret=get_my_key)
def callable_secret_task() -> int:
    return 42


@patch("flytekitplugins.asqav.tracking.asqav")
@patch("flytekitplugins.asqav.tracking.FlyteContextManager")
@patch("flytekitplugins.asqav.tracking.Deck")
def test_callable_secret(deck_mock, manager_mock, asqav_mock):
    """Verify a callable secret is called to get the API key."""
    ctx_mock = Mock()
    ctx_mock.execution_state.is_local_execution.return_value = False
    ctx_mock.user_space_params.execution_id = "exec-002"
    manager_mock.current_context.return_value = ctx_mock

    agent_mock = MagicMock()
    agent_mock.sign.return_value = _make_signature_response("receipt-callable")
    asqav_mock.Agent.create.return_value = agent_mock

    result = callable_secret_task()

    assert result == 42
    asqav_mock.init.assert_called_with(api_key="callable-key-456")


@task
@asqav_audit(agent_name="env-agent")
def env_var_task() -> str:
    return "from-env"


@patch("flytekitplugins.asqav.tracking.asqav")
@patch("flytekitplugins.asqav.tracking.FlyteContextManager")
@patch("flytekitplugins.asqav.tracking.Deck")
def test_env_var_fallback(deck_mock, manager_mock, asqav_mock, monkeypatch):
    """Verify the ASQAV_API_KEY env var is used when no Secret or callable is provided."""
    monkeypatch.setenv("ASQAV_API_KEY", "env-key-789")

    ctx_mock = Mock()
    ctx_mock.execution_state.is_local_execution.return_value = False
    ctx_mock.user_space_params.execution_id = "exec-003"
    manager_mock.current_context.return_value = ctx_mock

    agent_mock = MagicMock()
    agent_mock.sign.return_value = _make_signature_response("receipt-env")
    asqav_mock.Agent.create.return_value = agent_mock

    result = env_var_task()

    assert result == "from-env"
    asqav_mock.init.assert_called_with(api_key="env-key-789")


def test_no_api_key_error():
    """Verify a clear error is raised when no API key source is available."""
    decorator = asqav_audit(agent_name="no-key-agent")

    with pytest.raises(ValueError, match="No Asqav API key found"):
        decorator.execute()

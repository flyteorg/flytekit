from hashlib import shake_256
from unittest.mock import patch, Mock
import pytest

from flytekit import Secret, task
from flytekitplugins.comet_ml import comet_ml_login
from flytekitplugins.comet_ml.tracking import (
    COMET_ML_CUSTOM_TYPE_VALUE,
    COMET_ML_EXECUTION_TYPE_VALUE,
    _generate_suffix_with_length_10,
    _generate_experiment_key,
)


secret = Secret(key="abc", group="xyz")


@pytest.mark.parametrize("experiment_key", [None, "abc123dfassfasfsafsafd"])
def test_extra_config(experiment_key):
    project_name = "abc"
    workspace = "my_workspace"

    comet_decorator = comet_ml_login(
        project_name=project_name,
        workspace=workspace,
        experiment_key=experiment_key,
        secret=secret
    )

    @comet_decorator
    def task():
        pass

    assert task.secret is secret
    extra_config = task.get_extra_config()

    if experiment_key is None:
        assert extra_config[task.LINK_TYPE_KEY] == COMET_ML_EXECUTION_TYPE_VALUE
        assert task.COMET_ML_EXPERIMENT_KEY_KEY not in extra_config

        suffix = _generate_suffix_with_length_10(project_name=project_name, workspace=workspace)
        assert extra_config[task.COMET_ML_URL_SUFFIX_KEY] == suffix

    else:
        assert extra_config[task.LINK_TYPE_KEY] == COMET_ML_CUSTOM_TYPE_VALUE
        assert extra_config[task.COMET_ML_EXPERIMENT_KEY_KEY] == experiment_key
        assert task.COMET_ML_URL_SUFFIX_KEY not in extra_config

    assert extra_config[task.COMET_ML_WORKSPACE_KEY] == workspace
    assert extra_config[task.COMET_ML_HOST_KEY] == "https://www.comet.com"


@task
@comet_ml_login(project_name="abc", workspace="my-workspace", secret=secret, log_code=False)
def train_model():
    pass


@patch("flytekitplugins.comet_ml.tracking.comet_ml")
def test_local_execution(comet_ml_mock):
    train_model()

    comet_ml_mock.login.assert_called_with(
        project_name="abc", workspace="my-workspace", log_code=False)


@task
@comet_ml_login(
    project_name="xyz",
    workspace="another-workspace",
    secret=secret,
    experiment_key="my-previous-experiment-key",
)
def train_model_with_experiment_key():
    pass


@patch("flytekitplugins.comet_ml.tracking.comet_ml")
def test_local_execution_with_experiment_key(comet_ml_mock):
    train_model_with_experiment_key()

    comet_ml_mock.login.assert_called_with(
        project_name="xyz",
        workspace="another-workspace",
        experiment_key="my-previous-experiment-key",
    )


@patch("flytekitplugins.comet_ml.tracking.os")
@patch("flytekitplugins.comet_ml.tracking.FlyteContextManager")
@patch("flytekitplugins.comet_ml.tracking.comet_ml")
def test_remote_execution(comet_ml_mock, manager_mock, os_mock):
    # Pretend that the execution is remote
    ctx_mock = Mock()
    ctx_mock.execution_state.is_local_execution.return_value = False

    ctx_mock.user_space_params.secrets.get.return_value = "this_is_the_secret"
    ctx_mock.user_space_params.execution_id.name = "my_execution_id"

    manager_mock.current_context.return_value = ctx_mock
    hostname = "a423423423afasf4jigl-fasj4321-0"
    os_mock.environ = {"HOSTNAME": hostname}

    project_name = "abc"
    workspace = "my-workspace"

    h = shake_256(f"{project_name}-{workspace}".encode("utf-8"))
    suffix = h.hexdigest(5)
    hostname_alpha = hostname.replace("-", "")
    experiment_key = f"{hostname_alpha}{suffix}"

    train_model()

    comet_ml_mock.login.assert_called_with(
        project_name="abc",
        workspace="my-workspace",
        api_key="this_is_the_secret",
        experiment_key=experiment_key,
        log_code=False,
    )
    ctx_mock.user_space_params.secrets.get.assert_called_with(key="abc", group="xyz")


def get_secret():
    return "my-comet-ml-api-key"


@task
@comet_ml_login(project_name="my_project", workspace="my_workspace", secret=get_secret)
def train_model_with_callable_secret():
    pass


@patch("flytekitplugins.comet_ml.tracking.os")
@patch("flytekitplugins.comet_ml.tracking.FlyteContextManager")
@patch("flytekitplugins.comet_ml.tracking.comet_ml")
def test_remote_execution_with_callable_secret(comet_ml_mock, manager_mock, os_mock):
    # Pretend that the execution is remote
    ctx_mock = Mock()
    ctx_mock.execution_state.is_local_execution.return_value = False

    manager_mock.current_context.return_value = ctx_mock
    hostname = "a423423423afasf4jigl-fasj4321-0"
    os_mock.environ = {"HOSTNAME": hostname}

    train_model_with_callable_secret()

    comet_ml_mock.login.assert_called_with(
        project_name="my_project",
        api_key="my-comet-ml-api-key",
        workspace="my_workspace",
        experiment_key=_generate_experiment_key(hostname, "my_project", "my_workspace")
    )

from unittest.mock import Mock, patch

import pytest
from flytekitplugins.wandb import wandb_init
from flytekitplugins.wandb.tracking import WANDB_CUSTOM_TYPE_VALUE, WANDB_EXECUTION_TYPE_VALUE

from flytekit import task


@pytest.mark.parametrize("id", [None, "abc123"])
def test_wandb_extra_config(id):
    wandb_decorator = wandb_init(
        project="abc",
        entity="xyz",
        secret_key="my-secret-key",
        id=id,
        host="https://my_org.wandb.org",
    )

    extra_config = wandb_decorator.get_extra_config()

    if id is None:
        assert extra_config[wandb_decorator.LINK_TYPE_KEY] == WANDB_EXECUTION_TYPE_VALUE
        assert wandb_decorator.WANDB_ID_KEY not in extra_config
    else:
        assert extra_config[wandb_decorator.LINK_TYPE_KEY] == WANDB_CUSTOM_TYPE_VALUE
        assert extra_config[wandb_decorator.WANDB_ID_KEY] == id
    assert extra_config[wandb_decorator.WANDB_HOST_KEY] == "https://my_org.wandb.org"


@task
@wandb_init(project="abc", entity="xyz", secret_key="my-secret-key", secret_group="my-secret-group", tags=["my_tag"])
def train_model():
    pass


@patch("flytekitplugins.wandb.tracking.wandb")
def test_local_execution(wandb_mock):
    train_model()

    wandb_mock.init.assert_called_with(project="abc", entity="xyz", id=None, tags=["my_tag"])


@task
@wandb_init(project="abc", entity="xyz", secret_key="my-secret-key", tags=["my_tag"], id="1234")
def train_model_with_id():
    pass


@patch("flytekitplugins.wandb.tracking.wandb")
def test_local_execution_with_id(wandb_mock):
    train_model_with_id()

    wandb_mock.init.assert_called_with(project="abc", entity="xyz", id="1234", tags=["my_tag"])


@patch("flytekitplugins.wandb.tracking.os")
@patch("flytekitplugins.wandb.tracking.FlyteContextManager")
@patch("flytekitplugins.wandb.tracking.wandb")
def test_non_local_execution(wandb_mock, manager_mock, os_mock):
    # Pretend that the execution is remote
    ctx_mock = Mock()
    ctx_mock.execution_state.is_local_execution.return_value = False

    ctx_mock.user_space_params.secrets.get.return_value = "this_is_the_secret"
    ctx_mock.user_space_params.execution_id.name = "my_execution_id"

    manager_mock.current_context.return_value = ctx_mock
    os_mock.environ = {}

    train_model()

    wandb_mock.init.assert_called_with(project="abc", entity="xyz", id="my_execution_id", tags=["my_tag"])
    ctx_mock.user_space_params.secrets.get.assert_called_with(key="my-secret-key", group="my-secret-group")
    assert os_mock.environ["WANDB_API_KEY"] == "this_is_the_secret"


def test_errors():
    with pytest.raises(ValueError, match="project must be set"):
        wandb_init()

    with pytest.raises(ValueError, match="entity must be set"):
        wandb_init(project="abc")

    with pytest.raises(ValueError, match="secret_key must be set"):
        wandb_init(project="abc", entity="xyz")

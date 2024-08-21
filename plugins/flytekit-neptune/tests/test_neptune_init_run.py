from unittest.mock import patch, Mock

from flytekit import Secret, task, current_context
from flytekit.core.context_manager import FlyteContextManager
from flytekitplugins.neptune import neptune_init_run
from flytekitplugins.neptune.tracking import _neptune_init_run_class

neptune_api_token = Secret(key="neptune_api_token", group="neptune_group")


def test_get_extra_config():

    @neptune_init_run(project="flytekit/project", secret=neptune_api_token, tags=["my-tag"])
    def my_task() -> bool:
        ...

    config = my_task.get_extra_config()
    assert config[my_task.NEPTUNE_HOST_KEY] == "https://app.neptune.ai"
    assert config[my_task.NEPTUNE_PROJECT_KEY] == "flytekit/project"


@task
@neptune_init_run(project="flytekit/project", secret=neptune_api_token, tags=["my-tag"])
def neptune_task() -> bool:
    ctx = current_context()
    return ctx.neptune_run is not None


@patch("flytekitplugins.neptune.tracking.neptune")
def test_local_project_and_init_run_kwargs(neptune_mock):
    neptune_exists = neptune_task()
    assert neptune_exists

    neptune_mock.init_run.assert_called_with(
        project="flytekit/project", tags=["my-tag"]
    )


class RunObjectMock(dict):
    def __init__(self):
        self._stop_called = False

    def stop(self):
        self._stop_called = True


@patch.object(_neptune_init_run_class, "_get_secret")
@patch.object(_neptune_init_run_class, "_is_local_execution")
@patch("flytekitplugins.neptune.tracking.neptune")
def test_remote_project_and_init_run_kwargs(
    neptune_mock,
    mock_is_local_execution,
    mock_get_secret,
    monkeypatch,
):
    # Pretend that the execution is remote
    mock_is_local_execution.return_value = False
    api_token = "this-is-my-api-token"
    mock_get_secret.return_value = api_token

    host_name = "ff59abade1e7f4758baf-mainmytask-0"
    execution_url = "https://my-host.com/execution_url"
    monkeypatch.setenv("HOSTNAME", host_name)
    monkeypatch.setenv("FLYTE_EXECUTION_URL", execution_url)

    run_mock = RunObjectMock()
    init_run_mock = Mock(return_value=run_mock)
    neptune_mock.init_run = init_run_mock

    neptune_task()

    init_run_mock.assert_called_with(project="flytekit/project", tags=["my-tag"], api_token=api_token)
    assert run_mock["flyte/execution_id"] == host_name
    assert run_mock["flyte/execution_url"] == execution_url


def test_get_secret_callable():
    def get_secret():
        return "abc-123"

    @neptune_init_run(project="flytekit/project", secret=get_secret, tags=["my-tag"])
    def my_task():
        pass

    ctx_mock = Mock()
    assert my_task._get_secret(ctx_mock) == "abc-123"


def test_get_secret_object():
    secret_obj = Secret(key="my_key", group="my_group")

    @neptune_init_run(project="flytekit/project", secret=secret_obj, tags=["my-tag"])
    def my_task():
        pass

    get_secret_mock = Mock(return_value="my-secret-value")
    ctx_mock = Mock()
    ctx_mock.user_space_params.secrets.get = get_secret_mock

    assert my_task._get_secret(ctx_mock) == "my-secret-value"
    get_secret_mock.assert_called_with(key="my_key", group="my_group")

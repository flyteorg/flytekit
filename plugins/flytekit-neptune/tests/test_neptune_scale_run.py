from unittest.mock import patch, Mock

from flytekit import Secret, task, current_context
from flytekitplugins.neptune import neptune_scale_run
from flytekitplugins.neptune.scale_tracking import _neptune_scale_run_class

neptune_api_token = Secret(key="neptune_api_token", group="neptune_group")


def test_get_extra_config():

    @neptune_scale_run(project="flytekit/project", secret=neptune_api_token, run_id="my-task")
    def my_task() -> bool: ...

    config = my_task.get_extra_config()
    assert config[my_task.NEPTUNE_PROJECT] == "flytekit/project"
    assert config[my_task.NEPTUNE_RUN_ID] == "my-task"

    @neptune_scale_run(
        project="flytekit/project",
        secret=neptune_api_token,
        experiment_name="my-experiment",
    )
    def my_task() -> bool: ...

    config = my_task.get_extra_config()
    assert config[my_task.NEPTUNE_PROJECT] == "flytekit/project"


@task
@neptune_scale_run(project="flytekit/project", secret=neptune_api_token)
def neptune_task() -> bool:
    ctx = current_context()
    return ctx.neptune_run is not None


@patch("flytekitplugins.neptune.scale_tracking.neptune_scale")
def test_local_project_and_init_run_kwargs(neptune_scale_mock):
    neptune_exists = neptune_task()
    assert neptune_exists

    neptune_scale_mock.Run.assert_called_with(
        run_id=None, experiment_name=None, project="flytekit/project"
    )


class RunObjectMock(dict):
    def __init__(self):
        self._close_called = False

    def close(self):
        self._close_called = True

    def log_configs(self, data: dict):
        pass


@patch.object(_neptune_scale_run_class, "_get_secret")
@patch.object(_neptune_scale_run_class, "_is_local_execution")
@patch("flytekitplugins.neptune.scale_tracking.neptune_scale")
def test_remote_project_and_init_run_kwargs(
    neptune_scale_mock,
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
    run_mock = Mock(return_value=run_mock)
    neptune_scale_mock.Run = run_mock

    neptune_task()

    run_mock.assert_called_with(
        project="flytekit/project",
        run_id=host_name,
        api_token=api_token,
        experiment_name=None,
    )


def test_get_secret_callable():
    def get_secret():
        return "abc-123"

    @neptune_scale_run(project="flytekit/project", secret=get_secret)
    def my_task():
        pass

    ctx_mock = Mock()
    assert my_task._get_secret(ctx_mock) == "abc-123"


def test_get_secret_object():
    secret_obj = Secret(key="my_key", group="my_group")

    @neptune_scale_run(project="flytekit/project", secret=secret_obj)
    def my_task():
        pass

    get_secret_mock = Mock(return_value="my-secret-value")
    ctx_mock = Mock()
    ctx_mock.user_space_params.secrets.get = get_secret_mock

    assert my_task._get_secret(ctx_mock) == "my-secret-value"
    get_secret_mock.assert_called_with(key="my_key", group="my_group")

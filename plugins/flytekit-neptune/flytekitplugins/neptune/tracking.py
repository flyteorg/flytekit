import os
from functools import partial
from typing import Callable, Union

import neptune
from flytekit import Secret
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.utils import ClassDecorator

NEPTUNE_RUN_VALUE = "neptune-run-id"


def neptune_init_run(
    project: str,
    secret: Union[Secret, Callable],
    host: str = "https://app.neptune.ai",
    **init_run_kwargs: dict,
):
    """Neptune plugin.

    Args:
        project (str): Name of the project where the run should go, in the form `workspace-name/project_name`.
            (Required)
        secret (Secret or Callable): Secret with your `NEPTUNE_API_KEY` or a callable that returns the API key.
            The callable takes no arguments and returns a string. (Required)
        host (str): URL to Neptune. Defaults to "https://app.neptune.ai".
        **init_run_kwargs (dict):
    """
    return partial(
        _neptune_init_run_class,
        project=project,
        secret=secret,
        host=host,
        **init_run_kwargs,
    )


class _neptune_init_run_class(ClassDecorator):
    NEPTUNE_HOST_KEY = "host"
    NEPTUNE_PROJECT_KEY = "project"

    def __init__(
        self,
        task_function: Callable,
        project: str,
        secret: Union[Secret, Callable],
        host: str = "https://app.neptune.ai",
        **init_run_kwargs: dict,
    ):
        """Neptune plugin. See `neptune_init_run` for documentation on the parameters.

        `neptune_init_run` is the public interface to enforce that `project` and `secret`
        must be passed in.
        """
        self.project = project
        self.secret = secret
        self.host = host
        self.init_run_kwargs = init_run_kwargs

        super().__init__(task_function, project=project, secret=secret, host=host, **init_run_kwargs)

    def _is_local_execution(self, ctx: FlyteContext) -> bool:
        return ctx.execution_state.is_local_execution()

    def _get_secret(self, ctx: FlyteContext) -> str:
        if isinstance(self.secret, Secret):
            secrets = ctx.user_space_params.secrets
            return secrets.get(key=self.secret.key, group=self.secret.group)
        else:
            # Callable
            return self.secret()

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = self._is_local_execution(ctx)

        init_run_kwargs = {"project": self.project, **self.init_run_kwargs}

        if not is_local_execution:
            init_run_kwargs["api_token"] = self._get_secret(ctx)

        run = neptune.init_run(**init_run_kwargs)

        if not is_local_execution:
            # The HOSTNAME is set to {.executionName}-{.nodeID}-{.taskRetryAttempt}
            # If HOSTNAME is not defined, use the execution name as a fallback
            hostname = os.environ.get("HOSTNAME", ctx.user_space_params.execution_id.name)
            # Execution specific metadata
            run["flyte/execution_id"] = hostname
            run["flyte/project"] = ctx.user_space_params.execution_id.project
            run["flyte/domain"] = ctx.user_space_params.execution_id.domain
            run["flyte/name"] = ctx.user_space_params.execution_id.name
            run["flyte/raw_output_prefix"] = ctx.user_space_params.raw_output_prefix
            run["flyte/output_metadata_prefix"] = ctx.user_space_params.output_metadata_prefix
            run["flyte/working_directory"] = ctx.user_space_params.working_directory

            # Task specific metadata
            run["flyte/task/name"] = ctx.user_space_params.task_id.name
            run["flyte/task/project"] = ctx.user_space_params.task_id.project
            run["flyte/task/domain"] = ctx.user_space_params.task_id.domain
            run["flyte/task/version"] = ctx.user_space_params.task_id.version

            if (execution_url := os.getenv("FLYTE_EXECUTION_URL")) is not None:
                run["flyte/execution_url"] = execution_url

        new_user_params = ctx.user_space_params.builder().add_attr("NEPTUNE_RUN", run).build()
        with FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.execution_state.with_params(user_space_params=new_user_params))
        ):
            output = self.task_function(*args, **kwargs)
            run.stop()
            return output

    def get_extra_config(self):
        return {
            self.NEPTUNE_HOST_KEY: self.host,
            self.NEPTUNE_PROJECT_KEY: self.project,
            self.LINK_TYPE_KEY: NEPTUNE_RUN_VALUE,
        }

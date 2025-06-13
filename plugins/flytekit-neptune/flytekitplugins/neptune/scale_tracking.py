import os
from functools import partial
from typing import Callable, Optional, Union

import neptune_scale

from flytekit import Secret
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.utils import ClassDecorator

NEPTUNE_RUN_VALUE = "neptune-scale-run"
NEPTUNE_RUN_CUSTOM_VALUE = "neptune-scale-custom-id"


def neptune_scale_run(
    project: str,
    secret: Union[Secret, Callable],
    run_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
    **init_run_kwargs: dict,
) -> "Callable[..., _neptune_scale_run_class]":
    """Neptune Scale Plugin.

    Args:
        project (str): Name of the project where the run should go, in the form `workspace-name/project_name`.
            (Required)
        secret (Union[Secret, Callable]): Secret with your `NEPTUNE_API_KEY` or a callable that returns the API key.
            The callable takes no arguments and returns a string. (Required)
        run_id (Optional[str]): A unique id for this Neptune run. If not provided, Neptune will generate its own id.
        experiment_name (Optional[str]): If provided, the run will be logged as an experiment with this name.
        **init_run_kwargs (dict): Additional keyword arguments to pass to the Neptune Run constructor.

    Returns:
        Callable[..., _neptune_scale_run_class]: A callable that returns a wrapped Neptune Scale run instance.
    """
    return partial(
        _neptune_scale_run_class,
        project=project,
        secret=secret,
        run_id=run_id,
        experiment_name=experiment_name,
        **init_run_kwargs,
    )


class _neptune_scale_run_class(ClassDecorator):
    NEPTUNE_PROJECT = "project"
    NEPTUNE_RUN_ID = "id"

    def __init__(
        self,
        task_function: Callable,
        project: str,
        secret: Union[Secret, Callable],
        run_id: Optional[str] = None,
        experiment_name: Optional[str] = None,
        **init_run_kwargs: dict,
    ):
        self.project = project
        self.secret = secret
        self.run_id = run_id
        self.experiment_name = experiment_name
        self.init_run_kwargs = init_run_kwargs

        super().__init__(
            task_function,
            project=project,
            secret=secret,
            run_id=run_id,
            experiment_name=experiment_name,
            **init_run_kwargs,
        )

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

        metadata = {}
        run_id = self.run_id

        if not is_local_execution:
            init_run_kwargs["api_token"] = self._get_secret(ctx)

            if not run_id:
                # The HOSTNAME is set to {.executionName}-{.nodeID}-{.taskRetryAttempt}[-{replica-type}-{replica-index}]
                # If HOSTNAME is not defined, use the execution name as a fallback
                run_id = os.environ.get("HOSTNAME", ctx.user_space_params.execution_id.name)

            metadata = {
                # Execution specific metadata
                "flyte/project": ctx.user_space_params.execution_id.project,
                "flyte/domain": ctx.user_space_params.execution_id.domain,
                "flyte/name": ctx.user_space_params.execution_id.name,
                "flyte/raw_output_prefix": ctx.user_space_params.raw_output_prefix,
                "flyte/working_directory": ctx.user_space_params.working_directory,
                # Task specific metadata
                "flyte/task/name": ctx.user_space_params.task_id.name,
                "flyte/task/project": ctx.user_space_params.task_id.project,
                "flyte/task/domain": ctx.user_space_params.task_id.domain,
                "flyte/task/version": ctx.user_space_params.task_id.version,
            }

            if execution_url := os.getenv("FLYTE_EXECUTION_URL"):
                metadata["flyte/execution_url"] = execution_url

        run = neptune_scale.Run(run_id=run_id, experiment_name=self.experiment_name, **init_run_kwargs)
        if metadata:
            run.log_configs(metadata)

        new_user_params = ctx.user_space_params.builder().add_attr("NEPTUNE_RUN", run).build()
        with FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.execution_state.with_params(user_space_params=new_user_params))
        ):
            output = self.task_function(*args, **kwargs)
            run.close()
            return output

    def get_extra_config(self):
        extra_config = {self.NEPTUNE_PROJECT: self.project}

        if self.run_id is None:
            link_type = NEPTUNE_RUN_VALUE
        else:
            extra_config[self.NEPTUNE_RUN_ID] = self.run_id
            link_type = NEPTUNE_RUN_CUSTOM_VALUE

        extra_config[self.LINK_TYPE_KEY] = link_type
        return extra_config

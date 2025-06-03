import os
from functools import partial
from typing import Callable, Optional, Union

from neptune_scale import Run

from flytekit import Secret
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.utils import ClassDecorator

NEPTUNE_RUN_VALUE = "neptune-scale-run-id"


def neptune_scale_init_run(
    project: str,
    secret: Union[Secret, Callable],
    run_id: Optional[str] = None,
    host: str = "https://scale.neptune.ai",
    **init_run_kwargs: dict,
) -> "Callable[..., _neptune_scale_init_run_class]":
    """Neptune plugin.

    Args:
        project (str): Name of the project where the run should go, in the form `workspace-name/project_name`.
            (Required)
        secret (Union[Secret, Callable]): Secret with your `NEPTUNE_API_KEY` or a callable that returns the API key.
            The callable takes no arguments and returns a string. (Required)
        run_id (Optional[str]): A unique id for this Neptune run. If not provided, Neptune will generate its own id.
        host (str): URL to Neptune. Defaults to "https://app.neptune.ai".
        **init_run_kwargs (dict): Additional keyword arguments to pass to the Neptune Run constructor.

    Returns:
        Callable[..., _neptune_scale_init_run_class]: A callable that returns a wrapped Neptune Scale run instance.
    """
    return partial(
        _neptune_scale_init_run_class,
        project=project,
        secret=secret,
        run_id=run_id,
        host=host,
        **init_run_kwargs,
    )


class _neptune_scale_init_run_class(ClassDecorator):
    NEPTUNE_RUN_URL = "run_url"

    def __init__(
        self,
        task_function: Callable,
        project: str,
        secret: Union[Secret, Callable],
        run_id: Optional[str] = None,
        host: str = "https://scale.neptune.ai",
        **init_run_kwargs: dict,
    ):
        self.project = project
        self.secret = secret
        self.run_id = run_id
        self.host = host
        self.init_run_kwargs = init_run_kwargs

        super().__init__(
            task_function,
            project=project,
            secret=secret,
            run_id=run_id,
            host=host,
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

        self.final_run_id = None
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
                "flyte/output_metadata_prefix": ctx.user_space_params.output_metadata_prefix,
                "flyte/working_directory": ctx.user_space_params.working_directory,
                # Task specific metadata
                "flyte/task/name": ctx.user_space_params.task_id.name,
                "flyte/task/project": ctx.user_space_params.task_id.project,
                "flyte/task/domain": ctx.user_space_params.task_id.domain,
                "flyte/task/version": ctx.user_space_params.task_id.version,
                "flyte/execution_url": (
                    execution_url if (execution_url := os.getenv("FLYTE_EXECUTION_URL")) is not None else None
                ),
            }

        run = Run(run_id=run_id, **init_run_kwargs)
        if metadata:
            run.log_configs(metadata)

        self.run_url = run.get_experiment_url() if run._experiment_name else run.get_run_url()

        new_user_params = ctx.user_space_params.builder().add_attr("NEPTUNE_RUN", run).build()
        with FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.execution_state.with_params(user_space_params=new_user_params))
        ):
            output = self.task_function(*args, **kwargs)
            run.close()
            return output

    def get_extra_config(self):
        return {
            self.LINK_TYPE_KEY: NEPTUNE_RUN_VALUE,
            self.NEPTUNE_RUN_URL: self.run_url,
        }

import os
from hashlib import shake_256
from typing import Callable, Optional, Union

import comet_ml
from flytekit import Secret
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.utils import ClassDecorator

COMET_ML_EXECUTION_TYPE_VALUE = "comet-ml-execution-id"
COMET_ML_CUSTOM_TYPE_VALUE = "comet-ml-custom-id"


def _generate_suffix_with_length_10(project_name: str, workspace: str) -> str:
    """Generate suffix from project_name + workspace."""
    h = shake_256(f"{project_name}-{workspace}".encode("utf-8"))
    # Using 5 generates a suffix with length 10
    return h.hexdigest(5)


def _generate_experiment_key(hostname: str, project_name: str, workspace: str) -> str:
    """Generate experiment key that comet_ml can use:

    1. Is alphanumeric
    2. 32 <= len(experiment_key) <= 50
    """
    # In Flyte, then hostname is set to {.executionName}-{.nodeID}-{.taskRetryAttempt}, where
    # - len(executionName) == 20
    # - 2 <= len(nodeId) <= 8
    # - 1 <= len(taskRetryAttempt)) <= 2 (In practice, retries does not go above 99)
    # Removing the `-` because it is not alphanumeric, the 23 <= len(hostname) <= 30
    # On the low end we need to add 10 characters to stay in the range acceptable to comet_ml
    hostname = hostname.replace("-", "")
    suffix = _generate_suffix_with_length_10(project_name, workspace)
    return f"{hostname}{suffix}"


class comet_ml_init(ClassDecorator):
    COMET_ML_PROJECT_NAME_KEY = "project_name"
    COMET_ML_WORKSPACE_KEY = "workspace"
    COMET_ML_EXPERIMENT_KEY_KEY = "experiment_key"
    COMET_ML_URL_SUFFIX_KEY = "link_suffix"

    def __init__(
        self,
        task_function: Optional[Callable] = None,
        project_name: Optional[str] = None,
        workspace: Optional[str] = None,
        experiment_key: Optional[str] = None,
        secret: Optional[Union[Secret, Callable]] = None,
        **init_kwargs: dict,
    ):
        """Comet plugin.
        Args:
            task_function (function, optional): The user function to be decorated. Defaults to None.
            project_name (str): Send your experiment to a specific project. (Required)
            workspace (str): Attach an experiment to a project that belongs to this workspace. (Required)
            experiment_key (str): Experiment key.
            secret (Secret or Callable): Secret with your `COMET_API_KEY` or a callable that returns the API key.
                The callable takes no arguments and returns a string. (Required)
            **init_kwargs (dict): The rest of the arguments are passed directly to `comet_ml.init`.
        """
        if project_name is None:
            raise ValueError("project_name must be set")
        if workspace is None:
            raise ValueError("workspace must be set")
        if secret is None:
            raise ValueError("secret must be set")

        self.project_name = project_name
        self.workspace = workspace
        self.experiment_key = experiment_key
        self.secret = secret
        self.init_kwargs = init_kwargs

        super().__init__(
            task_function,
            project_name=project_name,
            workspace=workspace,
            experiment_key=experiment_key,
            secret=secret,
            **init_kwargs,
        )

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = ctx.execution_state.is_local_execution()

        default_kwargs = self.init_kwargs
        init_kwargs = {
            "project_name": self.project_name,
            "workspace": self.workspace,
            **default_kwargs,
        }

        if is_local_execution:
            # For local execution, always use the experiment_key. If `self.experiment_key` is `None`, comet_ml
            # will generate it's own key
            init_kwargs["experiment_key"] = self.experiment_key
        else:
            # Get api key for remote execution
            if isinstance(self.secret, Secret):
                secrets = ctx.user_space_params.secrets
                comet_ml_api_key = secrets.get(key=self.secret.key, group=self.secret.group)
            else:
                comet_ml_api_key = self.secret()

            init_kwargs["api_key"] = comet_ml_api_key

            if self.experiment_key is None:
                # The HOSTNAME is set to {.executionName}-{.nodeID}-{.taskRetryAttempt}
                # If HOSTNAME is not defined, use the execution name as a fallback
                hostname = os.environ.get("HOSTNAME", ctx.user_space_params.execution_id.name)
                experiment_key = _generate_experiment_key(hostname, self.project_name, self.workspace)
            else:
                experiment_key = self.experiment_key

            init_kwargs["experiment_key"] = experiment_key

        comet_ml.init(**init_kwargs)
        output = self.task_function(*args, **kwargs)
        return output

    def get_extra_config(self):
        extra_config = {
            self.COMET_ML_PROJECT_NAME_KEY: self.project_name,
            self.COMET_ML_WORKSPACE_KEY: self.workspace,
        }

        if self.experiment_key is None:
            comet_ml_value = COMET_ML_EXECUTION_TYPE_VALUE
            suffix = _generate_suffix_with_length_10(self.project_name, self.workspace)
            extra_config[self.COMET_ML_URL_SUFFIX_KEY] = suffix
        else:
            comet_ml_value = COMET_ML_CUSTOM_TYPE_VALUE
            extra_config[self.COMET_ML_EXPERIMENT_KEY_KEY] = self.experiment_key

        extra_config[self.LINK_TYPE_KEY] = comet_ml_value
        return extra_config

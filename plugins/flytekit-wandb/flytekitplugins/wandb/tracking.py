import os
from typing import Callable, Optional

import wandb
from flytekit import Secret
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.utils import ClassDecorator

WANDB_EXECUTION_TYPE_VALUE = "wandb-execution-id"
WANDB_CUSTOM_TYPE_VALUE = "wandb-custom-id"


class wandb_init(ClassDecorator):
    WANDB_PROJECT_KEY = "project"
    WANDB_ENTITY_KEY = "entity"
    WANDB_ID_KEY = "id"
    WANDB_HOST_KEY = "host"

    def __init__(
        self,
        task_function: Optional[Callable] = None,
        project: Optional[str] = None,
        entity: Optional[str] = None,
        secret: Optional[Secret] = None,
        id: Optional[str] = None,
        host: str = "https://wandb.ai",
        **init_kwargs: dict,
    ):
        """Weights and Biases plugin.
        Args:
            task_function (function, optional): The user function to be decorated. Defaults to None.
            project (str): The name of the project where you're sending the new run. (Required)
            entity (str): An entity is a username or team name where you're sending runs. (Required)
            secret (Secret): Secret with your `WANDB_API_KEY`. (Required)
            id (str, optional): A unique id for this wandb run.
            host (str, optional): URL to your wandb service. The default is "https://wandb.ai".
            **init_kwargs (dict): The rest of the arguments are passed directly to `wandb.init`. Please see
                [the `wandb.init` docs](https://docs.wandb.ai/ref/python/init) for details.
        """
        if project is None:
            raise ValueError("project must be set")
        if entity is None:
            raise ValueError("entity must be set")
        if secret is None:
            raise ValueError("secret must be set")

        self.project = project
        self.entity = entity
        self.id = id
        self.init_kwargs = init_kwargs
        self.secret = secret
        self.host = host

        # All kwargs need to be passed up so that the function wrapping works for both
        # `@wandb_init` and `@wandb_init(...)`
        super().__init__(
            task_function,
            project=project,
            entity=entity,
            secret=secret,
            id=id,
            host=host,
            **init_kwargs,
        )

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = ctx.execution_state.is_local_execution()

        if is_local_execution:
            # For location execution, always use the id. If `self.id` is `None`, wandb
            # will generate it's own id.
            wand_id = self.id
        else:
            # Set secret for remote execution
            secrets = ctx.user_space_params.secrets
            os.environ["WANDB_API_KEY"] = secrets.get(key=self.secret.key, group=self.secret.group)
            if self.id is None:
                # The HOSTNAME is set to {.executionName}-{.nodeID}-{.taskRetryAttempt}
                # If HOSTNAME is not defined, use the execution name as a fallback
                wand_id = os.environ.get("HOSTNAME", ctx.user_space_params.execution_id.name)
            else:
                wand_id = self.id

        wandb.init(project=self.project, entity=self.entity, id=wand_id, **self.init_kwargs)
        output = self.task_function(*args, **kwargs)
        wandb.finish()
        return output

    def get_extra_config(self):
        extra_config = {
            self.WANDB_PROJECT_KEY: self.project,
            self.WANDB_ENTITY_KEY: self.entity,
            self.WANDB_HOST_KEY: self.host,
        }

        if self.id is None:
            wandb_value = WANDB_EXECUTION_TYPE_VALUE
        else:
            wandb_value = WANDB_CUSTOM_TYPE_VALUE
            extra_config[self.WANDB_ID_KEY] = self.id

        extra_config[self.LINK_TYPE_KEY] = wandb_value
        return extra_config

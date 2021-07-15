import typing

from flytekitplugins.sqlalchemy import SQLAlchemyTask

from flytekit.core.shim_task import ShimTaskExecutor
from flytekit.models import task as task_models


class SQLAlchemyTaskExecutor(ShimTaskExecutor[SQLAlchemyTask]):
    def execute_from_model(self, tt: task_models.TaskTemplate, **kwargs) -> typing.Any:
        raise AssertionError("Implementation of the executor is only available within its docker image.")

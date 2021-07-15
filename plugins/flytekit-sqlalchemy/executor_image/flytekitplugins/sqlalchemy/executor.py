import typing

import pandas as pd
from flytekitplugins.sqlalchemy import SQLAlchemyTask
from sqlalchemy import create_engine

from flytekit import current_context
from flytekit.core.shim_task import ShimTaskExecutor
from flytekit.models import task as task_models


class SQLAlchemyTaskExecutor(ShimTaskExecutor[SQLAlchemyTask]):
    def execute_from_model(self, tt: task_models.TaskTemplate, **kwargs) -> typing.Any:
        if tt.custom["secret_connect_args"] is not None:
            for key, secret in tt.custom["secret_connect_args"].items():
                value = current_context().secrets.get(secret.group, secret.key)
                tt.custom["connect_args"][key] = value

        engine = create_engine(tt.custom["uri"], connect_args=tt.custom["connect_args"], echo=False)
        print(f"Connecting to db {tt.custom['uri']}")

        interpolated_query = SQLAlchemyTask.interpolate_query(tt.custom["query_template"], **kwargs)
        print(f"Interpolated query {interpolated_query}")
        with engine.begin() as connection:
            df = pd.read_sql_query(interpolated_query, connection)
        return df

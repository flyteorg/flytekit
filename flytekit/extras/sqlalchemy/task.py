import os
import typing
from dataclasses import dataclass

import pandas as pd
import pymysql
from sqlalchemy import *
from sqlalchemy import create_engine
from typing import Any, Dict, Optional

from flytekit import FlyteContext, kwtypes
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.types.schema import FlyteSchema


@dataclass
class SQLAlchemyConfig(object):
    """
    Use this configuration to configure task. String should be standard
    sqlalchemy connector format
    (https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls).
    Database can be found:
      - within the container
      - or from a publicly accessible source

    Args:
        uri: default sqlalchemy connector
        connect_args: sqlalchemy kwarg overrides -- ex: host
        password_secret_group: group for loading password as a secret
        password_secret_name: name for loading passworf as a secret
    """

    uri: str
    connect_args: Optional[Dict[str, Any]] = None
    password_secret_group: Optional[str] = None
    password_secret_name: Optional[str] = None


class SQLAlchemyTask(PythonInstanceTask[SQLAlchemyConfig], SQLTask[SQLAlchemyConfig]):
    """
    Makes it possible to run client side SQLAlchemy queries that optionally return a FlyteSchema object

    TODO: How should we use pre-built containers for running portable tasks like this. Should this always be a
          referenced task type?
    """

    _SQLALCHEMY_TASK_TYPE = "sqlalchemy"

    def __init__(
        self,
        name: str,
        query_template: str,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        task_config: typing.Optional[SQLAlchemyConfig] = None,
        output_schema_type: typing.Optional[typing.Type[FlyteSchema]] = None,
        **kwargs,
    ):
        outputs = kwtypes(results=output_schema_type if output_schema_type else FlyteSchema)
        self._uri = task_config.uri
        self._connect_args = task_config.connect_args
        if task_config.password_secret_name is not None and task_config.password_secret_group is not None:
            import urllib.parse
            secret_pwd = flytekit.current_context().secrets.get(task_config.password_secret_group, task_config.password_secret_name)
            self._connect_args["password"] = urllib.parse.quote_plus(secret_pwd)

        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._SQLALCHEMY_TASK_TYPE,
            query_template=query_template,
            inputs=inputs,
            outputs=outputs,
            **kwargs,
        )

    @property
    def output_columns(self) -> typing.Optional[typing.List[str]]:
        c = self.python_interface.outputs["results"].column_names()
        return c if c else None

    def execute(self, **kwargs) -> typing.Any:
        engine = create_engine(self._uri, connect_args=self._connect_args, echo=False)
        print(f"Connecting to db {self._uri}")
        with engine.begin() as connection:
            df = pd.read_sql_query(self.get_query(**kwargs), connection)
        return df

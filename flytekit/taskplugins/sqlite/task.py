import contextlib
import os
import sqlite3
import tempfile
import typing
from dataclasses import dataclass
from enum import Enum

from flytekit import FlyteContext, kwtypes
from flytekit.annotated.base_sql_task import SQLTask
from flytekit.models import task as _task_models
from flytekit.plugins import pandas as pd
from flytekit.types import FlyteFile, FlyteSchema
from flytekit.types.flyte_file import unarchive_file


@dataclass
class SQLite3Config(object):
    """
    Use this configuration to configure if sqlite3 files are passed in dynamically into the task or
    to configure a path to a static sqlite3 compatible database file that is available either
      - within the container
      - or from a publicly downloadable source

    If the Mode is marked dynamic, then a variable called ``sqlite: FlyteFile[".sqlite"]`` will be automatically added
    to the input interface of the task. Failing to supply this at runtime will raise an Assertion

        Args:
            sqlite_db_mode: STATIC should be use for constant locations of file, dynamic to allow dynamically provided
                            sqlite3 file locations. The file will have to comply with FlyteFile[.db|.zip|.tar.gz|.bz2]
                            format
            static_file_path: default FlyteFile that will be downloaded on execute
            compressed: Boolean that indicates if the given file is a compressed archive
    """

    class Mode(Enum):
        STATIC = "static"
        """
        STATIC refers to a constant, statically configured reference.
        """
        DYNAMIC = "dynamic"
        """
        Dynamic refers to SQLite files will be dynamically injected into the task.
        """

    sqlite_db_mode: Mode = Mode.DYNAMIC
    static_file_uri: typing.Optional[str] = None
    compressed: bool = False


class SQLite3SelectTask(SQLTask[SQLite3Config]):
    _SQLITE_INPUT_FILE = "sqlite"

    def __init__(
        self,
        name: str,
        query_template: str,
        metadata: typing.Optional[_task_models.TaskMetadata] = None,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        task_config: typing.Optional[SQLite3Config] = None,
        output_schema_type: typing.Optional[typing.Type[FlyteSchema]] = None,
        *args,
        **kwargs,
    ):
        if not inputs:
            inputs = {}
        if task_config is not None and task_config.sqlite_db_mode == SQLite3Config.Mode.STATIC:
            if task_config.static_file_uri is None:
                raise ValueError("SQLite DB mode configured to be static, but, the static_file_path was not specified.")
        else:
            if self._SQLITE_INPUT_FILE in inputs:
                raise ValueError(f"Input name `{self._SQLITE_INPUT_FILE}` is reserved as an auto-injected input")
            inputs[self._SQLITE_INPUT_FILE] = FlyteFile
        if output_schema_type is None:
            output_schema_type = FlyteSchema
        outputs = kwtypes(results=output_schema_type)
        super().__init__(
            name, metadata, query_template, inputs, outputs=outputs, task_config=task_config, *args, **kwargs
        )

    def execute(self, **kwargs) -> typing.Any:
        with tempfile.TemporaryDirectory() as temp_dir:
            if self.task_config is not None and self.task_config.sqlite_db_mode == SQLite3Config.Mode.STATIC:
                ctx = FlyteContext.current_context()
                file_ext = os.path.basename(self.task_config.static_file_uri)
                local_path = os.path.join(temp_dir, file_ext)
                ctx.file_access.download(self.task_config.static_file_uri, local_path)
                if self.task_config.compressed:
                    local_path = unarchive_file(local_path, temp_dir)
            else:
                if self._SQLITE_INPUT_FILE not in kwargs:
                    raise AssertionError("``sqlite: FlyteFile`` is a required argument")
                in_file = kwargs[self._SQLITE_INPUT_FILE]
                in_file.download()
                local_path = in_file.path
                del kwargs[self._SQLITE_INPUT_FILE]
                if self.task_config.compressed:
                    local_path = unarchive_file(local_path, temp_dir)

            print(f"Connecting to db {local_path}")
            with contextlib.closing(sqlite3.connect(local_path)) as con:
                df = pd.read_sql_query(self.get_query(**kwargs), con)
                return df

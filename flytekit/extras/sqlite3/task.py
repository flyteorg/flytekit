import contextlib
import os
import shutil
import sqlite3
import tempfile
import typing
from dataclasses import dataclass

import pandas as pd

from flytekit import FlyteContext, kwtypes
from flytekit.annotated.base_sql_task import SQLTask
from flytekit.annotated.python_function_task import PythonInstanceTask
from flytekit.types.schema import FlyteSchema


def unarchive_file(local_path: str, to_dir: str):
    """
    Unarchive given archive and returns the unarchived file name. It is expected that only one file is unarchived.
    More than one file or 0 files will result in a ``RuntimeError``
    """
    archive_dir = os.path.join(to_dir, "_arch")
    shutil.unpack_archive(local_path, archive_dir)
    # file gets uncompressed into to_dir/_arch/*.*
    files = os.listdir(archive_dir)
    if not files or len(files) == 0 or len(files) > 1:
        raise RuntimeError(f"Uncompressed archive should contain only one file - found {files}!")
    return os.path.join(archive_dir, files[0])


@dataclass
class SQLite3Config(object):
    """
    Use this configuration to configure if sqlite3 files that should be loaded by the task. The file itself is
    considered as a database and hence is treated like a configuration
    The path to a static sqlite3 compatible database file can be
      - within the container
      - or from a publicly downloadable source

    Args:
        uri: default FlyteFile that will be downloaded on execute
        compressed: Boolean that indicates if the given file is a compressed archive. Supported file types are
                        [zip, tar, gztar, bztar, xztar]
    """

    uri: str
    compressed: bool = False


class SQLite3Task(PythonInstanceTask[SQLite3Config], SQLTask[SQLite3Config]):
    """
    Makes it possible to run client side SQLite3 queries that optionally return a FlyteSchema object

    TODO: How should we use pre-built containers for running portable tasks like this. Should this always be a
          referenced task type?
    """

    _SQLITE_TASK_TYPE = "sqlite"

    def __init__(
        self,
        name: str,
        query_template: str,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        task_config: typing.Optional[SQLite3Config] = None,
        output_schema_type: typing.Optional[typing.Type[FlyteSchema]] = None,
        **kwargs,
    ):
        if task_config is None or task_config.uri is None:
            raise ValueError("SQLite DB uri is required.")
        outputs = kwtypes(results=output_schema_type if output_schema_type else FlyteSchema)
        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._SQLITE_TASK_TYPE,
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
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = FlyteContext.current_context()
            file_ext = os.path.basename(self.task_config.uri)
            local_path = os.path.join(temp_dir, file_ext)
            ctx.file_access.download(self.task_config.uri, local_path)
            if self.task_config.compressed:
                local_path = unarchive_file(local_path, temp_dir)

            print(f"Connecting to db {local_path}")
            with contextlib.closing(sqlite3.connect(local_path)) as con:
                df = pd.read_sql_query(self.get_query(**kwargs), con)
                return df

from __future__ import annotations

import contextlib
import os
import shutil
import sqlite3
import tempfile
import typing
from dataclasses import dataclass

import pandas as pd
from flyteidl.core import tasks_pb2
from google.protobuf import json_format as _json_format

from flytekit import kwtypes
from flytekit.common import utils as common_utils
from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.context_manager import FlyteContext, SerializationSettings
from flytekit.core.python_auto_container import ExecutionContainer
from flytekit.core.python_function_task import PythonAutoContainerTask
from flytekit.models import task as _task_model
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


class SQLite3Container(ExecutionContainer):
    def get_command(self, settings: SerializationSettings):
        return []

    # image = "ghcr.io/flyteorg/flytekit-sqlite3:latest"
    image = "flytekit-sqlite3:123"

    @staticmethod
    def get_args():
        return [
            # The path to the pyflyte execute script is from the perspective of the custom task container,
            # not the user's workflow container.
            "/usr/local/bin/pyflyte-tt-execute",
            "--execution-container-location",
            f"{SQLite3Container.__module__}.{SQLite3Container.__name__}",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--task-template-path",
            "{{.taskTemplatePath}}",
        ]

    def get_container(self, settings: SerializationSettings, task: SQLite3Task) -> _task_model.Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=self.image,
            command=[],
            args=self.get_args(),
            data_loading_config=None,
            environment=env,
            storage_request=task.resources.requests.storage,
            cpu_request=task.resources.requests.cpu,
            gpu_request=task.resources.requests.gpu,
            memory_request=task.resources.requests.mem,
            storage_limit=task.resources.limits.storage,
            cpu_limit=task.resources.limits.cpu,
            gpu_limit=task.resources.limits.gpu,
            memory_limit=task.resources.limits.mem,
        )

    def run(self, inputs, output_prefix, raw_output_data_prefix, task_template_path):
        # Download the task template file
        ctx = FlyteContext.current_context()
        task_template_local_path = os.path.join(ctx.execution_state.working_dir, "task_template.pb")
        ctx.file_access.get_data(task_template_path, task_template_local_path)
        task_template_model = common_utils.load_proto_from_file(tasks_pb2.TaskTemplate, task_template_local_path)

        task_def = SQLite3Task.promote_from_idl(task_template_model)
        super().handle_annotated_task(task_def, inputs, output_prefix, raw_output_data_prefix)


class SQLite3Task(PythonAutoContainerTask[SQLite3Config], SQLTask[SQLite3Config]):
    """
    Makes it possible to run client side SQLite3 queries that optionally return a FlyteSchema object
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
        self._container = SQLite3Container()
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
    def container(self) -> ExecutionContainer:
        return self._container

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

    def get_custom(self, settings: SerializationSettings) -> typing.Dict[str, typing.Any]:
        return {
            "query_template": self.query_template,
            "inputs": "",
            "outputs": "",
            "uri": self.task_config.uri,
            "compressed": self.task_config.compressed,
        }

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        return self.container.get_container(settings, self)

    def get_command(self, settings: SerializationSettings):
        return []

    def get_target(self, settings: SerializationSettings) -> _task_model.Container:
        return self.container.get_container(settings, self)

    @classmethod
    def promote_from_idl(cls, tt: _task_model.TaskTemplate) -> SQLite3Task:
        custom = _json_format.MessageToDict(tt.custom)
        print(custom)
        qt = custom["query_template"]

        return cls(name=tt.id.name, query_template=qt, inputs=kwtypes(limit=int),
                   output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
                   task_config=SQLite3Config(
                       uri=custom["uri"],
                       compressed=True,
                   ))

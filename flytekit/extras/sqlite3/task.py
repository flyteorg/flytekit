import contextlib
import os
import shutil
import sqlite3
import tempfile
import typing
from dataclasses import dataclass
from typing import List, Optional, Type

import pandas as pd
from flyteidl.core import tasks_pb2 as _tasks_pb2

from flytekit import FlyteContext, kwtypes
from flytekit.common import utils as _common_utils
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_auto_container import TaskResolverMixin
from flytekit.core.python_third_party_task import PythonThirdPartyContainerTask
from flytekit.core.tracker import TrackedInstance
from flytekit.models import task as task_models
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


class TaskTemplateResolver(TrackedInstance, TaskResolverMixin):
    def __init__(self, task_class: Type[PythonThirdPartyContainerTask]):
        self._task_class = task_class
        super(TaskTemplateResolver, self).__init__()

    def name(self) -> str:
        return "task template resolver"

    def load_task(self, loader_args: List[str]) -> PythonTask:
        ctx = FlyteContext.current_context()
        task_template_local_path = os.path.join(ctx.execution_state.working_dir, "task_template.pb")
        ctx.file_access.get_data(loader_args[0], task_template_local_path)
        task_template_proto = _common_utils.load_proto_from_file(_tasks_pb2.TaskTemplate, task_template_local_path)
        task_template_model = task_models.TaskTemplate.from_flyte_idl(task_template_proto)
        return self._task_class(task_template=task_template_model)

    def loader_args(self, settings: SerializationSettings, t: PythonTask) -> List[str]:
        return ["{{.taskTemplatePath}}"]

    def get_all_tasks(self) -> List[PythonTask]:
        return []


class SQLite3Task(PythonThirdPartyContainerTask[SQLite3Config], SQLTask[SQLite3Config]):
    """
    Makes it possible to run client side SQLite3 queries that optionally return a FlyteSchema object

    TODO: How should we use pre-built containers for running portable tasks like this. Should this always be a
          referenced task type?
    """

    _SQLITE_TASK_TYPE = "sqlite"

    def __init__(
        self,
        task_template: Optional[task_models.TaskTemplate] = None,
        name: Optional[str] = None,
        query_template: Optional[str] = None,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        task_config: typing.Optional[SQLite3Config] = None,
        output_schema_type: typing.Optional[typing.Type[FlyteSchema]] = None,
        **kwargs,
    ):
        # Because in the task template case, we skip the rest of instantiation, unless we want the task template loading
        # behavior to also be present in all parent classes. Or we can just promote_from_model
        # self._task_type = "python-task"
        # self._name = name

        if task_template is not None:
            self._task_template = task_template
            print("Task template specified, skipping all other constructor logic...")
            return

        if task_config is None or task_config.uri is None:
            raise ValueError("SQLite DB uri is required.")
        outputs = kwtypes(results=output_schema_type if output_schema_type else FlyteSchema)
        super().__init__(
            name=name,
            task_config=task_config,
            container_image="flytekit-sqlite3:123",
            resolver=sqlite3_task_resolver,
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

    def get_custom(self, settings: SerializationSettings) -> typing.Dict[str, typing.Any]:
        return {
            "query_template": self.query_template,
            "inputs": "",
            "outputs": "",
            "uri": self.task_config.uri,
            "compressed": self.task_config.compressed,
        }

    def execute_from_model(self, tt: task_models.TaskTemplate, **kwargs) -> typing.Any:
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = FlyteContext.current_context()
            file_ext = os.path.basename(tt.custom["uri"])
            local_path = os.path.join(temp_dir, file_ext)
            ctx.file_access.download(tt.custom["uri"], local_path)
            if tt.custom["compressed"]:
                local_path = unarchive_file(local_path, temp_dir)

            print(f"Connecting to db {local_path}")
            interpolated_query = SQLite3Task.interpolate_query(tt.custom["query_template"], **kwargs)
            print(f"Interpolated query {interpolated_query}")
            with contextlib.closing(sqlite3.connect(local_path)) as con:
                df = pd.read_sql_query(interpolated_query, con)
                return df


sqlite3_task_resolver = TaskTemplateResolver(task_class=SQLite3Task)

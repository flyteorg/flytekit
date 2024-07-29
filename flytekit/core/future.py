from __future__ import annotations

import base64
import gzip
import hashlib
import pathlib
import tempfile
import typing
from datetime import timedelta
from typing import TYPE_CHECKING

import cloudpickle

if TYPE_CHECKING:
    from flytekit.core.base_task import PythonTask
    from flytekit.core.workflow import WorkflowBase
    from flytekit.remote.executions import FlyteWorkflowExecution
    from flytekit.tools.translator import Options


class FlyteFuture:
    def __init__(
        self,
        entity: typing.Union[PythonTask, WorkflowBase],
        version: typing.Optional[str] = None,
        options: typing.Optional[Options] = None,
        **kwargs,
    ):
        """A Future object that handling a Flyte task or workflow execution.

        This object requires the FlyteRemote client to be initialized before it can be used. The FlyteRemote client
        can be initialized by calling `flytekit.remote.init_remote()`.
        """
        from flytekit.remote.init_remote import REMOTE_DEFAULT_OPTIONS, REMOTE_ENTRY
        from flytekit.tools.script_mode import hash_file

        if REMOTE_ENTRY is None:
            raise Exception(
                "Remote Flyte client has not been initialized. Please call flytekit.remote.init_remote() before executing tasks."
            )
        self._remote_entry = REMOTE_ENTRY

        if version is None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                dest = pathlib.Path(tmp_dir, "pkl.gz")
                with gzip.GzipFile(filename=dest, mode="wb", mtime=0) as gzipped:
                    cloudpickle.dump(entity, gzipped)
                md5_bytes, _, _ = hash_file(dest)

                h = hashlib.md5(md5_bytes)
                version = base64.urlsafe_b64encode(h.digest()).decode("ascii").rstrip("=")

        if options is None:
            options = REMOTE_DEFAULT_OPTIONS

        self._version = version
        self._exe = self._remote_entry.execute(entity, version=version, inputs=kwargs, options=options)

    def wait(
        self,
        timeout: typing.Optional[timedelta] = None,
        poll_interval: typing.Optional[timedelta] = None,
        sync_nodes: bool = True,
    ) -> FlyteWorkflowExecution:
        return self._remote_entry.wait(
            self._exe,
            timeout=timeout,
            poll_interval=poll_interval,
            sync_nodes=sync_nodes,
        )

    @property
    def version(self) -> str:
        return self._version

    @property
    def exe(self) -> FlyteWorkflowExecution:
        return self._exe

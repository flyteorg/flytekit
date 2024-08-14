from __future__ import annotations

import base64
import gzip
import hashlib
import pathlib
import tempfile
import typing
from datetime import timedelta
from typing import TYPE_CHECKING

import click
import cloudpickle

if TYPE_CHECKING:
    from IPython.display import IFrame

    from flytekit.core.base_task import PythonTask
    from flytekit.core.type_engine import LiteralsResolver
    from flytekit.core.workflow import WorkflowBase
    from flytekit.remote.executions import FlyteWorkflowExecution
    from flytekit.tools.translator import Options


class FlyteFuture:
    def __init__(
        self,
        entity: typing.Union[PythonTask, WorkflowBase],
        options: typing.Optional[Options] = None,
        **kwargs,
    ):
        """A Future object that handling a Flyte task or workflow execution.

        This object requires the FlyteRemote client to be initialized before it can be used. The FlyteRemote client
        can be initialized by calling `flytekit.remote.init_remote()`.
        """
        from flytekit.core.base_task import PythonTask
        from flytekit.remote.init_remote import REMOTE_DEFAULT_OPTIONS, REMOTE_ENTRY
        from flytekit.tools.script_mode import hash_file

        if REMOTE_ENTRY is None:
            raise RuntimeError(
                "Remote Flyte client has not been initialized. Please call flytekit.remote.init_remote() before executing tasks."
            )
        self._remote_entry = REMOTE_ENTRY

        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = pathlib.Path(tmp_dir, "pkl.gz")
            with gzip.GzipFile(filename=dest, mode="wb", mtime=0) as gzipped:
                cloudpickle.dump(entity, gzipped)
            md5_bytes, _, _ = hash_file(dest)

            h = hashlib.md5(md5_bytes)
            version = base64.urlsafe_b64encode(h.digest()).decode("ascii").rstrip("=")

        if options is None:
            options = REMOTE_DEFAULT_OPTIONS

        self._exe = self._remote_entry.execute(entity, version=version, inputs=kwargs, options=options)
        self._is_task = isinstance(entity, PythonTask)
        console_url = self._remote_entry.generate_console_url(self._exe)
        s = (
            click.style("\n[✔] ", fg="green")
            + "Go to "
            + click.style(console_url, fg="cyan")
            + " to see execution in the console."
        )
        click.echo(s)

    def __wait(
        self,
        timeout: typing.Optional[timedelta] = None,
        poll_interval: typing.Optional[timedelta] = None,
        sync_nodes: bool = True,
    ) -> FlyteWorkflowExecution:
        """Wait for an execution of the task or workflow to complete.

        :param timeout: maximum amount of time to wait
        :param poll_interval: sync workflow execution at this interval
        :param sync_nodes: passed along to the sync call for the workflow execution
        """
        return self._remote_entry.wait(
            self._exe,
            timeout=timeout,
            poll_interval=poll_interval,
            sync_nodes=sync_nodes,
        )

    def get(
        self,
        timeout: typing.Optional[timedelta] = None,
    ) -> typing.Optional[LiteralsResolver]:
        """Wait for the execution to complete and return the output.

        :param timeout: maximum amount of time to wait
        """
        out = self.__wait(
            timeout=timeout,
            poll_interval=timedelta(seconds=5),
        )
        return out.outputs

    def get_deck(
        self,
        timeout: typing.Optional[timedelta] = None,
    ) -> typing.Optional[IFrame]:
        """Wait for the execution to complete and return the deck for the task.

        :param timeout: maximum amount of time to wait
        """
        if not self._is_task:
            raise ValueError("Deck can only be retrieved for task executions.")
        from IPython.display import IFrame

        self.__wait(
            timeout=timeout,
            poll_interval=timedelta(seconds=5),
        )
        for node_execution in self._exe.node_executions.values():
            uri = node_execution._closure.deck_uri
            if uri:
                break
        if uri == "":
            raise ValueError("Deck not found for task execution")

        deck_uri = self._remote_entry.client.get_download_signed_url(uri)
        return IFrame(src=deck_uri.signed_url, width=800, height=600)

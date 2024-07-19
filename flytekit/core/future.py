from __future__ import annotations
import typing
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flytekit.core.base_task import PythonTask
    from flytekit.core.workflow import WorkflowBase

class FlyteFuture:
    def __init__(self, 
                 entity: typing.Union[PythonTask, WorkflowBase],
                 version: typing.Optional[str] = None,
                 **kwargs):
        from flytekit.remote.init_remote import REMOTE_ENTRY
        if REMOTE_ENTRY is None:
            raise Exception("Remote Flyte client has not been initialized. Please call flytekit.remote.init_remote() before executing tasks.")
        self.remote_entry = REMOTE_ENTRY
        self.exe = self.remote_entry.execute(entity, version=version, inputs=kwargs)

    def wait(self, 
            timeout: typing.Optional[timedelta] = None,
            poll_interval: typing.Optional[timedelta] = None,
            sync_nodes: bool = True,
        ):
        return self.remote_entry.wait(self.exe, timeout=timeout, poll_interval=poll_interval, sync_nodes=sync_nodes)

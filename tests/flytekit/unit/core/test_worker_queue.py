import mock
import pytest
import asyncio

from flytekit.core.task import task
from flytekit.remote.remote import FlyteRemote
from flytekit.core.worker_queue import Controller
from flytekit.configuration import ImageConfig, LocalConfig, SerializationSettings
from flytekit.utils.asyn import loop_manager


@mock.patch("flytekit.core.worker_queue.Controller.launch_and_start_watch")
def test_controller(mock_start):
    @task
    def t1() -> str:
        return "hello"

    remote = FlyteRemote.for_sandbox()
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    def _mock_start(key: str, idx):
        entry = c.entries[key][idx]
        entry.wf_exec = mock.MagicMock()  # just to pass the assert
        entry.set_result("hello")

    mock_start.side_effect = _mock_start

    async def fake_eager():
        loop = asyncio.get_running_loop()
        f = c.add(loop, entity=t1, input_kwargs={})
        res = await f
        assert res == "hello"

    loop_manager.run_sync(fake_eager)

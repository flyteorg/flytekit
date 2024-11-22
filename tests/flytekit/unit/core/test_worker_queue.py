import mock
import pytest
import asyncio

from flytekit.core.task import task
from flytekit.remote.remote import FlyteRemote
from flytekit.core.worker_queue import Controller, WorkItem
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

    def _mock_start(wi: WorkItem, idx: int):
        assert c.entries[wi.entity.name][idx] is wi
        wi.wf_exec = mock.MagicMock()  # just to pass the assert
        wi.set_result("hello")

    mock_start.side_effect = _mock_start

    async def fake_eager():
        loop = asyncio.get_running_loop()
        f = c.add(loop, entity=t1, input_kwargs={})
        res = await f
        assert res == "hello"

    loop_manager.run_sync(fake_eager)


@pytest.mark.asyncio
@mock.patch("flytekit.core.worker_queue.Controller")
async def test_controller_launch(mock_controller):
    @task
    def t2() -> str:
        return "hello"

    def _mock_execute(
            entity,
            execution_name: str,
            inputs,
            version,
            image_config,
            options,
            envs,
    ):
        assert entity is t2
        assert execution_name.startswith("e-unit-test-t2-")
        assert envs == {'_F_EE_ROOT': 'exec-id'}
        print(entity, execution_name, inputs, version, image_config, options, envs)
        wf_exec = mock.MagicMock()
        return wf_exec

    remote = mock.MagicMock()
    remote.execute.side_effect = _mock_execute
    mock_controller.informer.watch.return_value = True

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    wi = WorkItem(t2, input_kwargs={}, fut=fut)

    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    c.launch_and_start_watch(wi, 0)
    assert wi.error is None

    wi.result = 5
    c.launch_and_start_watch(wi, 0)
    # Function shouldn't be called if item already has a result
    with pytest.raises(AssertionError):
        await fut


@pytest.mark.asyncio
async def test_wi():
    @task
    def t1() -> str:
        return "hello"

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    wi = WorkItem(t1, input_kwargs={}, fut=fut)

    with pytest.raises(AssertionError):
        wi.set_result("hello")

    assert not wi.ready

    wi.wf_exec = mock.MagicMock()
    wi.set_result("hello")
    assert wi.ready

    fut2 = loop.create_future()
    wi = WorkItem(t1, input_kwargs={}, fut=fut2)
    wi.set_error(ValueError("hello"))
    with pytest.raises(ValueError):
        await fut2

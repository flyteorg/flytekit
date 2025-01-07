import mock
import pytest
import asyncio
import datetime
from flytekit.core.task import task
from flytekit.remote.remote import FlyteRemote
from flytekit.core.worker_queue import Controller, WorkItem, ItemStatus, Update
from flytekit.configuration import ImageConfig, LocalConfig, SerializationSettings
from flytekit.utils.asyn import loop_manager
from flytekit.models.execution import ExecutionSpec, ExecutionClosure, ExecutionMetadata, NotificationList, Execution, AbortMetadata
from flytekit.models.core import identifier
from flytekit.models import common as common_models
from flytekit.models.core import execution


def _mock_reconcile(update: Update):
    update.status = ItemStatus.SUCCESS
    update.wf_exec = mock.MagicMock()
    # This is how the controller pulls the result from a successful execution
    update.wf_exec.outputs.as_python_native.return_value = "hello"


@mock.patch("flytekit.core.worker_queue.Controller.reconcile_one", side_effect=_mock_reconcile)
def test_controller(mock_reconcile):
    print(f"ID mock_reconcile {id(mock_reconcile)}")
    mock_reconcile.return_value = 123

    @task
    def t1() -> str:
        return "hello"

    remote = FlyteRemote.for_sandbox()
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    async def fake_eager():
        f = c.add(entity=t1, input_kwargs={})
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


def test_work_item_hashing_equality():
    from flytekit.remote import FlyteRemote, FlyteWorkflowExecution
    remote = FlyteRemote.for_sandbox(default_project="p", domain="d")

    e_spec = ExecutionSpec(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version"),
        ExecutionMetadata(ExecutionMetadata.ExecutionMode.MANUAL, "tester", 1),
        notifications=NotificationList(
            [
                common_models.Notification(
                    [execution.WorkflowExecutionPhase.ABORTED],
                    pager_duty=common_models.PagerDutyNotification(recipients_email=["a", "b", "c"]),
                )
            ]
        ),
        raw_output_data_config=common_models.RawOutputDataConfig(output_location_prefix="raw_output"),
        max_parallelism=100,
    )

    test_datetime = datetime.datetime(year=2022, month=1, day=1, tzinfo=datetime.timezone.utc)
    test_timedelta = datetime.timedelta(seconds=10)
    abort_metadata = AbortMetadata(cause="cause", principal="testuser")

    e_closure = ExecutionClosure(
        phase=execution.WorkflowExecutionPhase.SUCCEEDED,
        started_at=test_datetime,
        duration=test_timedelta,
        abort_metadata=abort_metadata,
    )

    e_id = identifier.WorkflowExecutionIdentifier("project", "domain", "exec-name")

    ex = Execution(id=e_id, spec=e_spec, closure=e_closure)

    fwe = FlyteWorkflowExecution.promote_from_model(ex, remote)

    @task
    def t1() -> str:
        return "hello"

    wi1 = WorkItem(entity=t1, wf_exec=fwe, input_kwargs={})
    wi2 = WorkItem(entity=t1, wf_exec=fwe, input_kwargs={})
    wi2.uuid = wi1.uuid
    assert wi1 == wi2

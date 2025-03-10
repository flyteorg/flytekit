import mock
import pytest
import datetime
from flytekit.core.task import task
from flytekit.remote.remote import FlyteRemote
from flytekit.core.worker_queue import Controller, WorkItem, ItemStatus, Update
from flytekit.configuration import ImageConfig, LocalConfig, SerializationSettings, Image
from flytekit.utils.asyn import loop_manager
from flytekit.models.execution import ExecutionSpec, ExecutionClosure, ExecutionMetadata, NotificationList, Execution, AbortMetadata
from flytekit.models.core import identifier
from flytekit.remote.executions import FlyteWorkflowExecution
from flytekit.models import common as common_models
from flytekit.models.core import execution
from flytekit.exceptions.eager import EagerException


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


@mock.patch("flytekit.core.worker_queue.Controller._execute")
def test_controller_launch(mock_thread_target):
    @task
    def t2() -> str:
        return "hello"

    def _mock_thread_target(*args, **kwargs):
        print("in thread")
    mock_thread_target.side_effect = _mock_thread_target

    wf_exec = mock.MagicMock()

    def _mock_execute(
            entity,
            execution_name: str,
            inputs,
            version,
            image_config,
            options,
            envs,
            serialization_settings,
    ):
        assert entity is t2
        assert execution_name.startswith("e-unit-test-t2-")
        assert envs == {'_F_EE_ROOT': 'exec-id'}
        assert serialization_settings == ss
        return wf_exec

    remote = mock.MagicMock()
    remote.execute.side_effect = _mock_execute

    wi = WorkItem(t2, input_kwargs={})

    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
        version="123",
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    response_wf_exec = c.launch_execution(wi, 0)
    assert response_wf_exec is wf_exec


@pytest.mark.asyncio
@mock.patch("flytekit.core.worker_queue.Controller.reconcile_one")
async def test_controller_update_cycle(mock_reconcile_one):
    """ Test the whole update cycle end to end """
    @task
    def t1() -> str:
        return "hello"

    remote = mock.MagicMock()
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
        version="123",
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    def _mock_reconcile_one(update: Update):
        print(f"in reconcile {update}")
        update.status = ItemStatus.SUCCESS
        update.wf_exec = mock.MagicMock()
        update.wf_exec.outputs.as_python_native.return_value = "hello"

    mock_reconcile_one.side_effect = _mock_reconcile_one

    add_coro = c.add(t1, input_kwargs={})
    res = await add_coro
    assert res == "hello"


@pytest.mark.asyncio
@mock.patch("flytekit.core.worker_queue.Controller._execute")
async def test_controller_update_cycle_get_items(mock_thread_target):
    """ Test just getting items to update """
    def _mock_thread_target(*args, **kwargs):
        print("in thread")
    mock_thread_target.side_effect = _mock_thread_target

    @task
    def t1() -> str:
        return "hello"

    wi = WorkItem(t1, input_kwargs={})
    wi2 = WorkItem(t1, input_kwargs={})

    remote = mock.MagicMock()
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
        version="123",
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    c.entries["t1"] = [wi, wi2]
    updates = c._get_update_items()
    assert len(updates) == 2
    update_items = iter(updates.items())
    uuid, update = next(update_items)
    assert uuid
    assert update.work_item is wi
    assert update.idx == 0
    assert update.status is None
    assert update.wf_exec is None
    assert update.error is None

    uuid_2, update_2 = next(update_items)
    assert uuid != uuid_2
    assert update_2.idx == 1


@pytest.mark.asyncio
@mock.patch("flytekit.core.worker_queue.Controller._execute")
async def test_controller_update_cycle_apply_updates(mock_thread_target):
    def _mock_thread_target(*args, **kwargs):
        print("in thread")
    mock_thread_target.side_effect = _mock_thread_target

    @task
    def t1() -> str:
        return "hello"

    wi = WorkItem(t1, input_kwargs={})
    wi2 = WorkItem(t1, input_kwargs={})

    remote = mock.MagicMock()
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
        version="123",
    )
    c = Controller(remote, ss, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")

    c.entries["t1"] = [wi, wi2]

    wf_exec_1 = mock.MagicMock()
    wf_exec_1.outputs.as_python_native.return_value = "hello"

    wf_exec_2 = mock.MagicMock()
    wf_exec_2.closure.error = Exception("closure error")

    update_items = {
        wi.uuid: Update(wi, 0, ItemStatus.SUCCESS, wf_exec_1, None),
        wi2.uuid: Update(wi2, 1, ItemStatus.FAILED, wf_exec_2, None),
    }

    c._apply_updates(update_items)
    assert c.entries["t1"][0].status == ItemStatus.SUCCESS
    assert c.entries["t1"][0].result == "hello"
    assert c.entries["t1"][1].status == ItemStatus.FAILED
    # errors in the closure are cast to eager exceptions
    assert isinstance(c.entries["t1"][1].error, EagerException)

    update_items_second = {
        wi2.uuid: Update(wi2, 1, ItemStatus.FAILED, wf_exec_2, ValueError("test value error")),
    }

    c._apply_updates(update_items_second)
    assert c.entries["t1"][0].status == ItemStatus.SUCCESS
    assert c.entries["t1"][0].result == "hello"
    assert c.entries["t1"][1].status == ItemStatus.FAILED
    # errors set on the update object itself imply issues with the local code and are returned as is.
    assert isinstance(c.entries["t1"][1].error, ValueError)


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

    fwex = FlyteWorkflowExecution.promote_from_model(ex, remote)

    @task
    def t1() -> str:
        return "hello"

    wi1 = WorkItem(entity=t1, wf_exec=fwex, input_kwargs={})
    wi2 = WorkItem(entity=t1, wf_exec=fwex, input_kwargs={})
    wi2.uuid = wi1.uuid
    assert wi1 == wi2


default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@pytest.mark.parametrize("phase,expected_update_status", [
    (execution.WorkflowExecutionPhase.SUCCEEDED, ItemStatus.SUCCESS),
    (execution.WorkflowExecutionPhase.FAILED, ItemStatus.FAILED),
    (execution.WorkflowExecutionPhase.ABORTED, ItemStatus.FAILED),
    (execution.WorkflowExecutionPhase.TIMED_OUT, ItemStatus.FAILED),
])
def test_reconcile(phase, expected_update_status):
    mock_remote = mock.MagicMock()
    wf_exec = FlyteWorkflowExecution(
        id=identifier.WorkflowExecutionIdentifier("project", "domain", "exec-name"),
        spec=ExecutionSpec(
            identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "project", "domain", "name", "version"),
            ExecutionMetadata(ExecutionMetadata.ExecutionMode.MANUAL, "tester", 1),
        ),
        closure=ExecutionClosure(
            phase=phase,
            started_at=datetime.datetime(year=2024, month=1, day=2, tzinfo=datetime.timezone.utc),
            duration=datetime.timedelta(seconds=10),
        )
    )
    mock_remote.sync_execution.return_value = wf_exec

    @task
    def t1():
        ...

    wi = WorkItem(entity=t1, wf_exec=wf_exec, input_kwargs={})
    c = Controller(mock_remote, serialization_settings, tag="exec-id", root_tag="exec-id", exec_prefix="e-unit-test")
    u = Update(wi, 0)
    c.reconcile_one(u)
    assert u.status == expected_update_status

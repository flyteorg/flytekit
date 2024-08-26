import os
import typing
from dataclasses import dataclass
from unittest import mock
from typing_extensions import Annotated, cast
from flytekitplugins.kfpytorch.task import Elastic

from flytekit import Artifact

import pytest
import torch
import torch.distributed as dist
from dataclasses_json import DataClassJsonMixin
from flytekitplugins.kfpytorch.task import CleanPodPolicy, Elastic, RunPolicy

import flytekit
from flytekit import task, workflow
from flytekit.core.context_manager import FlyteContext, FlyteContextManager, ExecutionState, ExecutionParameters, OutputMetadataTracker
from flytekit.configuration import SerializationSettings
from flytekit.exceptions.user import FlyteRecoverableException

@pytest.fixture(autouse=True, scope="function")
def restore_env():
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)

@dataclass
class Config(DataClassJsonMixin):
    lr: float = 1e-5
    bs: int = 64
    name: str = "foo"


def dist_communicate() -> int:
    """Communicate between distributed workers."""
    rank = torch.distributed.get_rank()
    world_size = dist.get_world_size()
    tensor = torch.tensor([5], dtype=torch.int64) + 2 * rank + world_size
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

    return tensor.item()


def train(config: Config) -> typing.Tuple[str, Config, torch.nn.Module, int]:
    """Mock training a model using torch-elastic for test purposes."""
    dist.init_process_group(backend="gloo")

    local_rank = os.environ["LOCAL_RANK"]

    out_model = torch.nn.Linear(1000, int(local_rank) + 1)
    config.name = "elastic-test"

    distributed_result = dist_communicate()

    return f"result from local rank {local_rank}", config, out_model, distributed_result


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_end_to_end(start_method: str) -> None:
    """Test that the workflow with elastic task runs end to end."""
    world_size = 2

    train_task = task(train,task_config=Elastic(nnodes=1, nproc_per_node=world_size, start_method=start_method))

    @workflow
    def wf(config: Config = Config()) -> typing.Tuple[str, Config, torch.nn.Module, int]:
        return train_task(config=config)

    r, cfg, m, distributed_result = wf()
    assert "result from local rank 0" in r
    assert cfg.name == "elastic-test"
    assert m.in_features == 1000
    assert m.out_features == 1
    """
    The distributed result is calculated by the workers of the elastic train
    task by performing a `dist.all_reduce` operation. The correct result can
    only be obtained if the distributed process group is initialized correctly.
    """
    assert distributed_result == sum([5 + 2 * rank + world_size for rank in range(world_size)])


@pytest.mark.parametrize(
    "start_method,target_exec_id,monkeypatch_exec_id_env_var",
    [
        ("spawn", "", False),
        ("spawn", "f12345678", True),
        ("fork", "local", False),
    ],
)
def test_execution_params(start_method: str, target_exec_id: str, monkeypatch_exec_id_env_var: bool, monkeypatch) -> None:
    """Test that execution parameters are set in the worker processes."""
    if monkeypatch_exec_id_env_var:
        monkeypatch.setenv("FLYTE_INTERNAL_EXECUTION_ID", target_exec_id)

    @task(task_config=Elastic(nnodes=1, nproc_per_node=1, start_method=start_method))
    def test_task(n: int):
        ctx = flytekit.current_context()

        assert ctx.execution_id.name == target_exec_id
        cp = ctx.checkpoint
        assert cp is not None

        cp.write(bytes(n + 1))
        return n + 1

    test_task(n=1)


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_rdzv_configs(start_method: str) -> None:
    """Test that rendezvous configs are passed to torch distributed."""
    from torch.distributed.launcher.api import LaunchConfig

    rdzv_configs = {"join_timeout": 10}

    @task(task_config=Elastic(nnodes=1,nproc_per_node=2,start_method=start_method,rdzv_configs=rdzv_configs))
    def test_task():
        pass

    with mock.patch("torch.distributed.launcher.api.LaunchConfig", side_effect=LaunchConfig) as mock_launch_config:
        test_task()
        assert mock_launch_config.call_args[1]["rdzv_configs"] == rdzv_configs


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_deck(start_method: str) -> None:
    """Test that decks created in the main worker process are transferred to the parent process."""
    world_size = 2

    @task(task_config=Elastic(nnodes=1, nproc_per_node=world_size, start_method=start_method), enable_deck=True)
    def train():
        import os

        ctx = flytekit.current_context()
        deck = flytekit.Deck("test-deck", f"Hello Flyte Deck viewer from worker process {os.environ.get('RANK')}",)
        ctx.decks.append(deck)
        default_deck = ctx.default_deck
        default_deck.append("Hello from default deck")

    @workflow
    def wf():
        train()

    wf()

    ctx = flytekit.current_context()

    expected_deck_names = {"Timeline", "default", "test-deck"}
    found_deck_names = set(d.name for d in ctx.decks)

    assert expected_deck_names.issubset(found_deck_names)

    default_deck = [d for d in ctx.decks if d.name == "default"][0]
    assert "Hello from default deck" == default_deck.html.strip()

    test_deck = [d for d in ctx.decks if d.name == "test-deck"][0]
    assert "Hello Flyte Deck viewer from worker process 0" in test_deck.html


class Card(object):
    def __init__(self, text: str):
        self.text = text

    def serialize_to_string(self, ctx: FlyteContext, variable_name: str):
        print(f"In serialize_to_string: {id(ctx)}")
        return "card", "card"


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_output_metadata_passing(start_method: str) -> None:
    ea = Artifact(name="elastic-artf")

    @task(
        task_config=Elastic(start_method=start_method),
    )
    def train2() -> Annotated[str, ea]:
        return ea.create_from("hello flyte", Card("## card"))

    @workflow
    def wf():
        train2()

    ctx = FlyteContext.current_context()
    omt = OutputMetadataTracker()
    with FlyteContextManager.with_context(ctx.with_execution_state(ctx.new_execution_state().with_params(mode=ExecutionState.Mode.LOCAL_TASK_EXECUTION)).with_output_metadata_tracker(omt)) as child_ctx:
        cast(ExecutionParameters, child_ctx.user_space_params)._decks = []
        # call execute directly so as to be able to get at the same FlyteContext object.
        res = train2.execute()
        om = child_ctx.output_metadata_tracker.get(res)
        assert len(om.additional_items) == 1


@pytest.mark.parametrize(
    "recoverable,start_method",
    [
        (True, "spawn"),
        (False, "spawn"),
        (True, "fork"),
        (False, "fork"),
    ],
)
def test_recoverable_error(recoverable: bool, start_method: str) -> None:
    """Test that recoverable errors are propagated from the workers to the agent process."""
    world_size = 2

    class CustomRecoverableException(FlyteRecoverableException):
        pass

    @task(task_config=Elastic(nnodes=1, nproc_per_node=world_size, start_method=start_method))
    def train(recoverable: bool):
        if recoverable:
            raise CustomRecoverableException("Recoverable error")
        else:
            raise Exception("Non-recoverable error")

    @workflow
    def wf(recoverable: bool):
        return train(recoverable=recoverable)

    if recoverable:
        with pytest.raises(FlyteRecoverableException):
            wf(recoverable=recoverable)
    else:
        with pytest.raises(RuntimeError):
            wf(recoverable=recoverable)


def test_default_timeouts():
    """Test that default timeouts are set for the elastic task."""
    @task(task_config=Elastic(nnodes=1))
    def test_task():
        pass

    assert test_task.task_config.rdzv_configs == {"join_timeout": 900, "timeout": 900}

def test_run_policy() -> None:
    """Test that run policy is propagated to custom spec."""

    run_policy = RunPolicy(
        clean_pod_policy=CleanPodPolicy.ALL,
        ttl_seconds_after_finished=10 * 60,
        active_deadline_seconds=36000,
        backoff_limit=None,
    )

    # nnodes must be > 1 to get pytorchjob spec
    @task(task_config=Elastic(nnodes=2, nproc_per_node=2, run_policy=run_policy))
    def test_task():
        pass

    spec = test_task.get_custom(SerializationSettings(image_config=None))

    assert spec["runPolicy"] == {
        "cleanPodPolicy": "CLEANPOD_POLICY_ALL",
        "ttlSecondsAfterFinished": 600,
        "activeDeadlineSeconds": 36000,
    }


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_omp_num_threads(start_method: str) -> None:
    """Test that the env var OMP_NUM_THREADS is set by default and not overwritten if set."""

    @task(task_config=Elastic(nnodes=1, nproc_per_node=2, start_method=start_method))
    def test_task_omp_default():
        assert os.environ["OMP_NUM_THREADS"] == "1"

    test_task_omp_default()

    os.environ["OMP_NUM_THREADS"] = "42"

    @task(task_config=Elastic(nnodes=1, nproc_per_node=2, start_method=start_method))
    def test_task_omp_set():
        assert os.environ["OMP_NUM_THREADS"] == "42"

    test_task_omp_set()

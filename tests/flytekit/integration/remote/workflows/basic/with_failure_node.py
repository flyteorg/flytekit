import typing

import flytekit as fl
from flytekit import WorkflowFailurePolicy
from flytekit.types.error.error import FlyteError


@fl.task
def create_cluster(name: str):
    print(f"Creating cluster: {name}")


@fl.task
def t1(a: int, b: str):
    print(f"{a} {b}")
    raise ValueError("Fail!")


@fl.task
def clean_up(name: str, err: typing.Optional[FlyteError] = None):
    print(f"Deleting cluster {name} due to {err}")


@fl.workflow(
    on_failure=clean_up,
    failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE,
)
def wf(name: str = "my_workflow"):
    c = create_cluster(name=name)
    t = t1(a=1, b="2")
    c >> t

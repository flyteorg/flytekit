# Copyright (C) 2015-2021 Blackshark.ai GmbH. All Rights reserved. www.blackshark.ai
from flytekit import task, workflow


def test_task_correctly_wrapped():
    @task
    def my_task(a: int) -> int:
        return a

    assert my_task.__wrapped__ == my_task._task_function


def test_wf_correctly_wrapped():
    @workflow
    def my_workflow(a: int) -> int:
        return a

    assert my_workflow.__wrapped__ == my_workflow._workflow_function

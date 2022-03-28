import pytest

from flytekit.configuration import Config
from flytekit.models.core.identifier import ResourceType
from flytekit.remote.remote import FlyteRemote

CLIENT_METHODS = {
    ResourceType.WORKFLOW: "list_workflows_paginated",
    ResourceType.TASK: "list_tasks_paginated",
    ResourceType.LAUNCH_PLAN: "list_launch_plans_paginated",
}

REMOTE_METHODS = {
    ResourceType.WORKFLOW: "fetch_workflow",
    ResourceType.TASK: "fetch_task",
    ResourceType.LAUNCH_PLAN: "fetch_launch_plan",
}

ENTITY_TYPE_TEXT = {
    ResourceType.WORKFLOW: "Workflow",
    ResourceType.TASK: "Task",
    ResourceType.LAUNCH_PLAN: "Launch Plan",
}


@pytest.mark.sandbox_test
def test_fetch_one_wf():
    rr = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_sandbox"),
        default_project="flytesnacks",
        default_domain="development",
    )
    wf = rr.fetch_workflow(name="core.flyte_basics.files.rotate_one_workflow", version="v0.3.56")
    # rr.recent_executions(wf)

    print("\nRotate one Workflow ------------------------------------")
    print(str(wf))
    print("====================================")


@pytest.mark.sandbox_test
def test_get_parent_wf_run():
    rr = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_sandbox"),
        default_project="flytesnacks",
        default_domain="development",
    )
    we = rr.fetch_workflow_execution(name="vudmhuxb9b")
    rr.sync_workflow_execution(we, sync_nodes=True)
    print(we)


@pytest.mark.sandbox_test
def test_get_merge_sort_run():
    rr = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_sandbox"),
        default_project="flytesnacks",
        default_domain="development",
    )
    we = rr.fetch_workflow_execution(name="djdo2l2s0s")
    rr.sync_workflow_execution(we, sync_nodes=True)
    print(we)


@pytest.mark.sandbox_test
def test_fetch_merge_sort():
    rr = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_sandbox"),
        default_project="flytesnacks",
        default_domain="development",
    )
    wf = rr.fetch_workflow(name="core.control_flow.run_merge_sort.merge_sort", version="v0.3.56")
    # rr.recent_executions(wf)

import os

import mock
from flyteidl.admin import launch_plan_pb2, workflow_pb2

from flytekit.configuration import Config
from flytekit.core.utils import load_proto_from_file
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.remote.remote import FlyteRemote

rr = FlyteRemote(
    Config.for_sandbox(),
    default_project="flytesnacks",
    default_domain="development",
)

responses_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "responses")


@mock.patch("flytekit.remote.remote.FlyteRemote.client")
def test_fetch_wf_wf_lp_pattern(mock_client):
    leaf_lp = load_proto_from_file(
        launch_plan_pb2.LaunchPlan,
        os.path.join(responses_dir, "admin.launch_plan_pb2.LaunchPlan_core.control_flow.subworkflows.leaf_subwf.pb"),
    )
    leaf_lp = launch_plan_models.LaunchPlan.from_flyte_idl(leaf_lp)
    root_wf = load_proto_from_file(
        workflow_pb2.Workflow,
        os.path.join(responses_dir, "admin.workflow_pb2.Workflow_core.control_flow.subworkflows.root_level_wf.pb"),
    )
    root_wf = admin_workflow_models.Workflow.from_flyte_idl(root_wf)

    mock_client.get_workflow.return_value = root_wf
    mock_client.get_launch_plan.return_value = leaf_lp
    fwf = rr.fetch_workflow(name="core.control_flow.subworkflows.root_level_wf", version="JiepXcXB3SiEJ8pwYDy-7g==")
    assert len(fwf.sub_workflows) == 2


@mock.patch("flytekit.remote.remote.FlyteRemote.client")
def test_fetch_wf_wf_lp_pattern(mock_client):
    ...


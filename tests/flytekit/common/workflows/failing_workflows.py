from __future__ import absolute_import, division, print_function

from flytekit.sdk import tasks as _tasks, workflow as _workflow
from flytekit.sdk.tasks import python_task
from flytekit.sdk.types import Types as _Types
from flytekit.sdk.workflow import workflow_class, Input, Output
from flytekit.models.core.workflow import WorkflowMetadata


@python_task
def div_zero(wf_params):
    return 5 / 0


@workflow_class(on_failure=WorkflowMetadata.OnFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
class FailingWorkflowWithRunToCompletion(object):
    """
    [start] ->  [first_layer] -> [second_layer]   ->  [end]
            \\_  [first_layer_2]                      _/
    """

    first_layer = div_zero()
    first_layer_2 = div_zero()
    second_layer = div_zero()

    # This forces second_layer node to run after first layer
    first_layer >> second_layer

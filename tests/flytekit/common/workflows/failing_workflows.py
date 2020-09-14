from flytekit.models.core.workflow import WorkflowMetadata
from flytekit.sdk.tasks import python_task
from flytekit.sdk.workflow import workflow_class


@python_task
def div_zero(wf_params):
    return 5 / 0


@python_task
def log_something(wf_params):
    wf_params.logging.warn("Hello world")


@workflow_class(on_failure=WorkflowMetadata.OnFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
class FailingWorkflowWithRunToCompletion(object):
    """
    [start] ->  [first_layer] -> [second_layer]   ->  [end]
            \\_  [first_layer_2]                      _/
    """

    first_layer = log_something()
    first_layer_2 = div_zero()
    second_layer = div_zero()

    # This forces second_layer node to run after first layer
    first_layer >> second_layer

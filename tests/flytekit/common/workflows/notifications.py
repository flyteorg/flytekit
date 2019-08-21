from __future__ import absolute_import, print_function

from flytekit.sdk.tasks import python_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input
from flytekit.common import notifications as _notifications
from flytekit.models.core import execution as _execution


@inputs(a=Types.Integer, b=Types.Integer)
@outputs(c=Types.Integer)
@python_task
def add_two_integers(wf_params, a, b, c):
    c.set(a + b)


@workflow_class
class BasicWorkflow(object):
    input_1 = Input(Types.Integer)
    input_2 = Input(Types.Integer, default=1, help='Not required.')
    a = add_two_integers(a=input_1, b=input_2)


notification_lp = BasicWorkflow.create_launch_plan(notifications=[
    _notifications.Email([_execution.WorkflowExecutionPhase.SUCCEEDED, _execution.WorkflowExecutionPhase.FAILED,
                          _execution.WorkflowExecutionPhase.TIMED_OUT, _execution.WorkflowExecutionPhase.ABORTED],
                         ['flyte-test-notifications@mydomain.com'])
])

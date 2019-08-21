from __future__ import absolute_import, print_function

from flytekit.sdk.tasks import python_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input, Output


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task(gpu_request="1", gpu_limit="1")
def add_one(wf_params, a, b):
    # TODO lets add a test that works with tensorflow, but we need it to be in
    # a different container
    b.set(a + 1)


@workflow_class
class SimpleWorkflow(object):
    input_1 = Input(Types.Integer)
    input_2 = Input(Types.Integer, default=5, help='Not required.')
    a = add_one(a=input_1)
    output = Output(a.outputs.b, sdk_type=Types.Integer)

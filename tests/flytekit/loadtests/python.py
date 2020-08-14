from __future__ import absolute_import, division, print_function

import time

from six.moves import range

from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class


@inputs(value1_to_add=Types.Integer, value2_to_add=Types.Integer)
@outputs(out=Types.Integer)
@python_task(cpu_request="5", cpu_limit="5", memory_request="32G")
def sum_and_print(workflow_parameters, value1_to_add, value2_to_add, out):
    for i in range(11 * 60):
        print("This is load test task. I have been running for {} seconds.".format(i))
        time.sleep(1)

    summed = sum([value1_to_add, value2_to_add])
    print("Summed up to: {}".format(summed))
    out.set(summed)


@workflow_class
class FlytePythonLoadTestWorkflow(object):
    print_sum = [None] * 30
    for i in range(0, 30):
        print_sum[i] = sum_and_print(value1_to_add=1, value2_to_add=1)

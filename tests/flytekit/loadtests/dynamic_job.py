import time

from six.moves import range

from flytekit.sdk.tasks import dynamic_task, inputs, outputs, python_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, workflow_class


@inputs(value1=Types.Integer)
@outputs(out=Types.Integer)
@python_task(cpu_request="1", cpu_limit="1", memory_request="5G")
def dynamic_sub_task(workflow_parameters, value1, out):
    for i in range(11 * 60):
        print("This is load test task. I have been running for {} seconds.".format(i))
        time.sleep(1)

    output = value1 * 2
    print("Output: {}".format(output))
    out.set(output)


@inputs(tasks_count=Types.Integer)
@outputs(out=[Types.Integer])
@dynamic_task(cache_version="1")
def dynamic_task(workflow_parameters, tasks_count, out):
    res = []
    for i in range(0, tasks_count):
        task = dynamic_sub_task(value1=i)
        yield task
        res.append(task.outputs.out)

    # Define how to set the final result of the task
    out.set(res)


@workflow_class
class FlyteDJOLoadTestWorkflow(object):
    tasks_count = Input(Types.Integer)
    dj = dynamic_task(tasks_count=tasks_count)

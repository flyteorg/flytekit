from __future__ import absolute_import, division, print_function

from flytekit.sdk.tasks import python_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input


@inputs(value_to_print=Types.Integer)
@outputs(out=Types.Integer)
@python_task(cache_version='1')
def add_one_and_print(workflow_parameters, value_to_print, out):
    workflow_parameters.stats.incr("task_run")
    added = value_to_print + 1
    print("My printed value: {}".format(added))
    out.set(added)


@inputs(value1_to_print=Types.Integer, value2_to_print=Types.Integer)
@outputs(out=Types.Integer)
@python_task(cache_version='1')
def sum_non_none(workflow_parameters, value1_to_print, value2_to_print, out):
    workflow_parameters.stats.incr("task_run")
    added = 0
    for value in [value1_to_print, value2_to_print]:
        print("Adding values: {}".format(value))
        if value is not None:
            added += value
    added += 1
    print("My printed value: {}".format(added))
    out.set(added)


@inputs(value1_to_add=Types.Integer, value2_to_add=Types.Integer, value3_to_add=Types.Integer,
        value4_to_add=Types.Integer)
@outputs(out=Types.Integer)
@python_task(cache_version='1')
def sum_and_print(workflow_parameters, value1_to_add, value2_to_add, value3_to_add, value4_to_add, out):
    workflow_parameters.stats.incr("task_run")
    summed = sum([value1_to_add, value2_to_add, value3_to_add, value4_to_add])
    print("Summed up to: {}".format(summed))
    out.set(summed)


@inputs(value_to_print=Types.Integer, date_triggered=Types.Datetime)
@python_task(cache_version='1')
def print_every_time(workflow_parameters, value_to_print, date_triggered):
    workflow_parameters.stats.incr("task_run")
    print("My printed value: {} @ {}".format(value_to_print, date_triggered))


@workflow_class
class PythonTasksWorkflow(object):
    triggered_date = Input(Types.Datetime)
    print1a = add_one_and_print(value_to_print=3)
    print1b = add_one_and_print(value_to_print=101)
    print2 = sum_non_none(value1_to_print=print1a.outputs.out,
                          value2_to_print=print1b.outputs.out)
    print3 = add_one_and_print(value_to_print=print2.outputs.out)
    print4 = add_one_and_print(value_to_print=print3.outputs.out)
    print_sum = sum_and_print(
        value1_to_add=print2.outputs.out,
        value2_to_add=print3.outputs.out,
        value3_to_add=print4.outputs.out,
        value4_to_add=100
    )
    print_always = print_every_time(
        value_to_print=print_sum.outputs.out,
        date_triggered=triggered_date)

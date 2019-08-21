from __future__ import absolute_import, print_function

from flytekit.sdk.tasks import python_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input, Output


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task
def add_one(wf_params, a, b):
    b.set(a + 1)


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task
def subtract_one(wf_params, a, b):
    b.set(a - 1)


@inputs(a=Types.Integer, b=Types.Integer)
@outputs(c=Types.Integer)
@python_task
def sum(wf_params, a, b, c):
    c.set(a + b)


@workflow_class
class Child(object):
    input_1 = Input(Types.Integer)
    input_2 = Input(Types.Integer, default=5, help='Not required.')
    a = add_one(a=input_1)
    b = add_one(a=input_2)
    c = add_one(a=100)
    output = Output(c.outputs.b, sdk_type=Types.Integer)


# Create a simple launch plan without any overrides for inputs to the child workflow.
child_lp = Child.create_launch_plan()


# Create a parent workflow that invokes the child workflow previously declared.  Note that it takes advantage of
# default and required inputs on different calls.
@workflow_class
class Parent(object):
    input_1 = Input(Types.Integer)
    child1 = child_lp(input_1=input_1)
    child2 = child_lp(input_1=input_1, input_2=10)
    final_sum = sum(a=child1.outputs.output, b=child2.outputs.output)
    output = Output(final_sum.outputs.c, sdk_type=Types.Integer)

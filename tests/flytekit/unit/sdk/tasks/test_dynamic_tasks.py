from __future__ import absolute_import
from __future__ import print_function

from six import moves as _six_moves

from flytekit.common.tasks import sdk_runnable as _sdk_runnable, sdk_dynamic as _sdk_dynamic
from flytekit.sdk.tasks import inputs, outputs, dynamic_task, python_task
from flytekit.sdk.types import Types


@inputs(in1=Types.Integer)
@outputs(out_str=[Types.String], out_ints=[[Types.Integer]])
@dynamic_task
def sample_batch_task(wf_params, in1, out_str, out_ints):
    res = ["I'm the first result"]
    for i in _six_moves.range(0, in1):
        task = sub_task(in1=i)
        yield task
        res.append(task.outputs.out1)
        res.append("I'm after each sub-task result")
    res.append("I'm the last result")

    res2 = []
    for i in _six_moves.range(0, in1):
        task = int_sub_task(in1=i)
        yield task
        res2.append(task.outputs.out1)

    # Nested batch tasks
    task = sample_batch_task_sq()
    yield task
    res2.append(task.outputs.out_ints)

    task = sample_batch_task_sq()
    yield task
    res2.append(task.outputs.out_ints)

    out_str.set(res)
    out_ints.set(res2)


@outputs(out_ints=[Types.Integer])
@dynamic_task
def sample_batch_task_sq(wf_params, out_ints):
    res2 = []
    for i in _six_moves.range(0, 3):
        task = sq_sub_task(in1=i)
        yield task
        res2.append(task.outputs.out1)
    out_ints.set(res2)


@outputs(out_str=[Types.String], out_ints=[[Types.Integer]])
@dynamic_task
def sample_batch_task_no_inputs(wf_params, out_str, out_ints):
    res = ["I'm the first result"]
    for i in _six_moves.range(0, 3):
        task = sub_task(in1=i)
        yield task
        res.append(task.outputs.out1)
        res.append("I'm after each sub-task result")
    res.append("I'm the last result")

    res2 = []
    for i in _six_moves.range(0, 3):
        task = int_sub_task(in1=i)
        yield task
        res2.append(task.outputs.out1)

    # Nested batch tasks
    task = sample_batch_task_sq()
    yield task
    res2.append(task.outputs.out_ints)

    task = sample_batch_task_sq()
    yield task
    res2.append(task.outputs.out_ints)

    out_str.set(res)
    out_ints.set(res2)


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@python_task
def sub_task(wf_params, in1, out1):
    out1.set("hello {}".format(in1))


@inputs(in1=Types.Integer)
@outputs(out1=[Types.Integer])
@python_task
def int_sub_task(wf_params, in1, out1):
    wf_params.stats.incr("int_sub_task")
    out1.set([in1, in1 * 2, in1 * 3])


@inputs(in1=Types.Integer)
@outputs(out1=Types.Integer)
@python_task
def sq_sub_task(wf_params, in1, out1):
    out1.set(in1 * in1)


@inputs(in1=Types.Integer)
@outputs(out_str=[Types.String])
@dynamic_task
def no_future_batch_task(wf_params, in1, out_str):
    out_str.set(["res1", "res2"])


def test_batch_task():
    assert isinstance(sample_batch_task, _sdk_runnable.SdkRunnableTask)
    assert isinstance(sample_batch_task, _sdk_dynamic._SdkDynamicTask)

    expected = {
        'out_str': ["I'm the first result", 'hello 0', "I'm after each sub-task result", 'hello 1',
                    "I'm after each sub-task result", 'hello 2', "I'm after each sub-task result",
                    "I'm the last result"],
        'out_ints': [[0, 0, 0], [1, 2, 3], [2, 4, 6], [0, 1, 4], [0, 1, 4]]
    }

    res = sample_batch_task.unit_test(in1=3)
    assert expected == res


def test_no_future_batch_task():
    expected = {
        'out_str': ["res1", "res2"]
    }

    res = no_future_batch_task.unit_test(in1=3)
    assert expected == res

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six import moves as _six_moves

from flytekit.sdk.tasks import python_task, inputs, outputs, dynamic_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Output, Input


@outputs(out_ints=[Types.Integer])
@dynamic_task
def sample_batch_task_sq(wf_params, out_ints):
    wf_params.stats.incr("task_run")
    res2 = []
    for i in _six_moves.range(0, 3):
        task = sq_sub_task(in1=i)
        yield task
        res2.append(task.outputs.out1)
    out_ints.set(res2)


@outputs(out_str=[Types.String], out_ints=[[Types.Integer]])
@dynamic_task
def no_inputs_sample_batch_task(wf_params, out_str, out_ints):
    wf_params.stats.incr("task_run")
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
@outputs(out_str=[Types.String])
@dynamic_task(cache=True, cache_version='1')
def sample_batch_task_beatles_cached(wf_params, in1, out_str):
    wf_params.stats.incr("task_run")
    res2 = []
    for i in _six_moves.range(0, in1):
        task = sample_beatles_lyrics_cached(in1=i)
        yield task
        res2.append(task.outputs.out1)
    out_str.set(res2)


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@python_task(cache=True, cache_version='1')
def sample_beatles_lyrics_cached(wf_params, in1, out1):
    wf_params.stats.incr("task_run")
    lyrics = ["Ob-La-Di, Ob-La-Da", "When I'm 64", "Yesterday"]
    out1.set(lyrics[in1 % 3])


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@python_task
def sub_task(wf_params, in1, out1):
    wf_params.stats.incr("task_run")
    out1.set("hello {}".format(in1))


@inputs(in1=Types.Integer)
@outputs(out1=[Types.Integer])
@python_task
def int_sub_task(wf_params, in1, out1):
    wf_params.stats.incr("task_run")
    out1.set([in1, in1 * 2, in1 * 3])


@inputs(in1=Types.Integer)
@outputs(out1=Types.Integer)
@python_task
def sq_sub_task(wf_params, in1, out1):
    wf_params.stats.incr("task_run")
    out1.set(in1 * in1)


@inputs(ints_to_print=[[Types.Integer]], strings_to_print=[Types.String])
@python_task(cache_version='1')
def print_every_time(wf_params, ints_to_print, strings_to_print):
    wf_params.stats.incr("task_run")
    print("Expected Int values: {}".format([[0, 0, 0], [1, 2, 3], [2, 4, 6], [0, 1, 4], [0, 1, 4]]))
    print("Actual Int values: {}".format(ints_to_print))

    print("Expected String values: {}".format(
        [u"I'm the first result", u"hello 0", u"I'm after each sub-task result", u'hello 1',
         u"I'm after each sub-task result", u'hello 2', u"I'm after each sub-task result", u"I'm the last result"]))
    print("Actual String values: {}".format(strings_to_print))


@workflow_class
class BatchTasksWorkflow(object):
    num_subtasks = Input(Types.Integer, default=3)
    task1 = no_inputs_sample_batch_task()
    task2 = sample_batch_task_beatles_cached(in1=num_subtasks)
    t = print_every_time(ints_to_print=task1.outputs.out_ints, strings_to_print=task1.outputs.out_str)
    ints_out = Output(task1.outputs.out_ints, sdk_type=[[Types.Integer]])
    str_out = Output(task2.outputs.out_str, sdk_type=[Types.String])

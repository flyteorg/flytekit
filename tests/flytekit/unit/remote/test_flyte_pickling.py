import tempfile
from flytekit.core.dynamic_workflow_task import dynamic
import typing
import cloudpickle
import pickle
from .example import t1


@dynamic
def my_subwf(a: int) -> typing.List[str]:
    s = []
    for i in range(a):
        s.append(t1(a=i))
    return s


"""
Define task
FlyteRemote.register_task
First pickle
Upload the pickle
Create the serialized task object

At run time, the task resolver needs to download the pickle
load from the downloaded file instead of 
"""


def test_basic_pickling():
    x = cloudpickle.dumps(my_subwf)
    p_dyn = pickle.loads(x)
    res = p_dyn(a=5)
    assert res == ['fast-2', 'fast-3', 'fast-4', 'fast-5', 'fast-6']


def test_pickle_through_buffer():
    x = cloudpickle.dumps(my_subwf)

    with tempfile.TemporaryFile() as fh:
        fh.write(x)
        fh.seek(0)

        p_dyn_task_b = fh.read()

    p_dyn_task = cloudpickle.loads(p_dyn_task_b)
    res = p_dyn_task(a=5)
    assert res == ['fast-2', 'fast-3', 'fast-4', 'fast-5', 'fast-6']


def test_to_file():
    ...
    # x = cloudpickle.dumps(my_subwf)
    #
    # with open("/Users/ytong/dyn.pickle", "wb") as fh:
    #     fh.write(x)

    # with open("/Users/ytong/dyn.pickle", "rb") as fh:
    #     p_dyn_task_b = fh.read()
    #
    # p_dyn_task = cloudpickle.loads(p_dyn_task_b)
    # print(p_dyn_task(a=5))

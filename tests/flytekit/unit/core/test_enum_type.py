from enum import Enum

from flytekit import task, workflow


def test_dynamic_local():
    class Color(Enum):
        RED = 'red'
        GREEN = 'green'
        BLUE = 'blue'

    @task
    def my_task(c: Color) -> Color:
        print(c)
        return c

    @workflow
    def wf(c: Color) -> Color:
        return my_task(c=c)

    res = wf(c=Color.RED)
    assert res == Color.RED

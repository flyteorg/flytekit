import enum

from flytekit import task


class Color(enum.Enum):
    RED = 'red'
    GREEN = 'green'
    BLUE = 'blue'


@task
def foo(i: int = 0, j: str = "Hello", c: Color = Color.RED):
    print(i, j, c)

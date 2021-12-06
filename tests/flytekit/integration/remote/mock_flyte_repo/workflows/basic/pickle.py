import typing

from flytekit import task, workflow


class Foo(object):
    def __init__(self, number: int):
        self.number = number


@task
def t1(a: int) -> Foo:
    return Foo(number=a)


@task
def t2(a: Foo) -> typing.List[Foo]:
    return [a, a]


@task
def t3(a: typing.List[Foo]) -> typing.Dict[str, Foo]:
    return {"hello": a[0]}


@workflow
def wf(a: int) -> typing.Dict[str, Foo]:
    o1 = t1(a=a)
    o2 = t2(a=o1)
    return t3(a=o2)


if __name__ == "__main__":
    print(f"Running wf(a=3) {wf(a=3)}")

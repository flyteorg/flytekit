from flytekit.core.task import task


@task
def t1(a: int) -> str:
    a = a + 2
    return "fast-" + str(a)

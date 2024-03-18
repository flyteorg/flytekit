from flytekit import task, workflow


@task(cache=True, cache_version="v1", cache_ignore_input_vars=["a"])
def add(a: int, b: int) -> int:
    return a + b

@workflow
def add_wf(a: int, b: int) -> int:
    return add(a=a, b=b)

if __name__ == "__main__":
    assert add_wf(a=10, b=5) == 15
    assert add_wf(a=20, b=5) == 15  # since a is ignored, this line will hit cache of a=10, b=5
    assert add_wf(a=20, b=8) == 28

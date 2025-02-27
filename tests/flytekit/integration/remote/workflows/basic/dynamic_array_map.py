from flytekit import workflow, task, map_task, dynamic


@task
def fn(x: int) -> int:
    return x + 1


@dynamic
def dynamic(data: list[int]) -> list[int]:
    return map_task(fn)(x=data)


@workflow
def workflow_with_maptask_in_dynamic(data: list[int]) -> list[int]:
    return dynamic(data=data)

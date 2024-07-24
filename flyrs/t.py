from flytekit import ImageSpec, StructuredDataset, kwtypes, task, workflow


@task
def say_hi() -> str:
    return "hi"

@workflow
def say_hi_wf():
    say_hi()
    return

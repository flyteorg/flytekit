from flytekit.annotated.stuff import task, workflow, WorkflowOutputs


@task(outputs=['s_out'])
def x(s: int) -> int:
    return s + 1


@workflow(outputs=["real_b"])
def my_workflow() -> WorkflowOutputs:
    # a = x(s=3)
    b = x(s=x(s=3))
    return WorkflowOutputs(b)

from flytekit.annotated.stuff import task, workflow, WorkflowOutputs
from flytekit.annotated.type_engine import outputs


@task
def x(s: int) -> outputs(s_out=int):
    return s + 1


# @workflow
# def my_workflow() -> outputs(real_b=int):
#     # a = x(s=3)
#     b = x(s=x(s=3))
#     return WorkflowOutputs(b)

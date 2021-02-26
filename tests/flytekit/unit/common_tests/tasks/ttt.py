from flytekit.common.types import primitives
from flytekit.sdk.tasks import inputs, outputs, python_task


@inputs(a=primitives.Integer)
@outputs(b=primitives.Integer)
@python_task
def my_task(wf_params, a, b):
    b.set(a + 1)

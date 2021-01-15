from flytekit.legacy.sdk.tasks import inputs, outputs, python_task
from flytekit.legacy.sdk import Types


@inputs(a=Types.Integer)
@outputs(b=Types.Integer)
@python_task
def add_one(wf_params, a, b):
    b.set(a + 1)

from flytekit.core.task import task, eager
from flytekit.configuration import ImageConfig, SerializationSettings, Image
from flytekit.core.task import task, eager


@task
def add_one(x: int) -> int:
    return x + 1


@eager
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)
    return out

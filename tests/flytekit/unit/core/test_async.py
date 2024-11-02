from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.experimental.eager_function import eager


@task
def add_one(x: int) -> int:
    return x + 1


# @task
# async def double(x: int) -> int:
#     return x * 2


@eager
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)
    return out

    # This is the pattern for async tasks.
    # doubled = double(x=x)
    # other_double = double(x=x)
    # doubled, other_double = asyncio.gather(doubled, other_double)
    # if out - doubled < 0:
    #     return -1
    # return await double(x=out)


def test_easy_1():
    print('hi')
    res = simple_eager_workflow(x=1)
    print(res)

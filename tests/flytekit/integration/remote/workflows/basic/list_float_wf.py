import typing
import os

from flytekit import task, workflow

IMAGE = os.environ.get("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")


@task(container_image=IMAGE)
def concat_list(xs: typing.List[float]) -> str:
    return f'[{", ".join([str(x) for x in xs])}]'


@workflow
def my_list_float_wf(xs: typing.List[float]) -> str:
    return concat_list(xs=xs)


if __name__ == "__main__":
    print(f"Running my_wf(xs=[1.0, 0.99]) {my_list_float_wf(xs=[1.0, 0.99])}")

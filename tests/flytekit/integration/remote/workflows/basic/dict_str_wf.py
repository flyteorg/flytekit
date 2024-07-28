import json
import typing
import os

from flytekit import task, workflow

IMAGE = os.getenv("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")


@task(container_image=IMAGE)
def convert_to_string(d: typing.Dict[str, str]) -> str:
    return json.dumps(d)


@workflow
def my_dict_str_wf(d: typing.Dict[str, str]) -> str:
    return convert_to_string(d=d)


if __name__ == "__main__":
    print(f"Running my_wf(d={{'a': 'rwx', 'b': '42'}})={my_dict_str_wf(d={'a': 'rwx', 'b': '42'})}")

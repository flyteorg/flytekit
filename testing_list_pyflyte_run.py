from typing import List

from flytekit import task, workflow


@task
def process_list(my_list: List[int]) -> int:
    return sum(my_list)


@workflow
def wf(my_list: List[int]) -> int:
    return process_list(my_list=my_list)


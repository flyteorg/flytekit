from flytekit import task
from flytekit.core.tracker import extract_task_module

from .decorator_source import task_setup


@task
@task_setup
def get_data(x: int) -> int:
    print("running get_data")
    return x + 1

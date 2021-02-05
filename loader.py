from typing import List

from flytekit.core.context_manager import InstanceVar
from flytekit.core.python_function_task import PythonFunctionTask, PythonInstanceTask, TaskLoader
from flytekit.core.workflow import Workflow
from flytekit import workflow


class Builder(TaskLoader):
    def __init__(self):
        self.d = {}
        self.reverse = {}

    def name(self) -> str:
        return "Builder"

    def get_all_tasks(self) -> List[PythonInstanceTask]:
        return list(self.reverse.keys())

    def add(self, x: str):
        def foo(x: int) -> int:
            return x

        fn = PythonFunctionTask(task_config=None, task_function=foo, allow_nested=True, task_loader=self)
        self.d[x] = fn
        self.reverse[fn] = x

    def load_task(self, loader_args: List[str]) -> PythonInstanceTask:
        if len(loader_args) != 1:
            raise RuntimeError(f"Unable to load task, received ambiguous loader args {loader_args}, expected only one")
        return self.d[loader_args[0]]

    def loader_args(self, var: InstanceVar, for_task: PythonInstanceTask) -> List[str]:
        return [self.reverse[for_task]]

    def build(self) -> Workflow:
        self.add("x")
        self.add("y")
        @workflow
        def foo():
            pass

        return foo


# Note you have to have the builder available, else the tasks wont be loaded
builder = Builder()
wf = builder.build()


if __name__ == "__main__":
    tk = builder.get_all_tasks()

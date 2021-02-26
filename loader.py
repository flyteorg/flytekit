from typing import List

from flytekit import task, workflow
from flytekit.core.context_manager import InstanceVar, TaskResolverMixin
from flytekit.core.python_function_task import PythonFunctionTask, PythonInstanceTask
from flytekit.core.workflow import Workflow


class Builder(TaskResolverMixin):
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

        fn = PythonFunctionTask(task_config=None, task_function=foo, task_resolver=self)
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


@workflow
def my_workflow(i: int) -> int:
    # This works, but with a important caveat, my_workflow variables `i` should not be used inside the task. this will
    # cause implicit binding and will fail at runtime. The registration will be wrong! Not sure how to detect this.
    @task
    def foo(j: int) -> int:
        return j

    return foo(j=i)


if __name__ == "__main__":
    tk = builder.get_all_tasks()

    print(my_workflow.get_all_tasks()[0].name)
    print(my_workflow.load_task(["__main__.foo"])(j=10))

from typing import List, Callable

from flytekit.core.context_manager import InstanceVar
from flytekit.core.python_auto_container import TaskResolverMixin, PythonAutoContainerTask
from flytekit.core.python_function_task import PythonFunctionTask, PythonInstanceTask
from flytekit.core.workflow import Workflow
from flytekit import workflow, task
from flytekit.core.context_manager import SerializationSettings


@task
def tsk_write(in1: str):
    print(f"Write task {in1}")


# class Builder:
#     def __init__(self, query: str):
#         self.do_fn = None
#         self.query = query
#
#     @staticmethod
#     def create(query: str = None):
#         return Builder(query)
#
#     def assign_function(self, do_fn):
#         self.do_fn = do_fn
#         return self
#
#     def build(self):
#         @task
#         def my_task() -> str:
#             print(f"In nested task, query is {self.query}")
#             self.do_fn()
#             return "hello from the inner function"
#
#         @workflow
#         def my_wf() -> None:
#             inner_task_output = my_task()
#             tsk_write(in1=inner_task_output)
#
#         return my_wf, my_task


# Single task workflow builder
class Builder(TaskResolverMixin):
    def __init__(self):
        self.d = {}
        self.reverse = {}

    def name(self) -> str:
        return "Builder"

    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        return list(self.reverse.keys())

    def add(self, user_function: Callable):
        fn = PythonFunctionTask(task_config=None, task_function=user_function, task_resolver=self)
        print(user_function)
        self.d[user_function] = fn
        self.reverse[fn] = user_function

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        if len(loader_args) != 1:
            raise RuntimeError(f"Unable to load task, received ambiguous loader args {loader_args}, expected only one")
        return self.d[loader_args[0]]

    def loader_args(self, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        """
        This is responsible for turning an instance of a task into args that the load_task function can reconstitute.

        At run time, pyflyte-execute will instantiate the given resolver

            b = Builder()

        and then call b.load_task(loader_args)
        """
        if t not in self.reverse:
            raise Exception("no such task")

        return []

    @staticmethod
    def build() -> Workflow:
        @workflow
        def foo():
            pass

        return foo

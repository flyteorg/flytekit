from collections import OrderedDict
from typing import Callable, List

from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_auto_container import PythonAutoContainerTask, TaskResolverMixin
from flytekit.core.tracker import TrackedInstance
from flytekit.core.python_function_task import PythonFunctionTask


class ClassStorageTaskResolver(TrackedInstance, TaskResolverMixin):
    """
    Stores tasks inside a class variable. The class must be inherited from at the point of usage because the task
    loading process basically relies on the same sequence of things happening.
    """
    def __init__(self):
        self.mapping = OrderedDict()
        super().__init__()

    def name(self) -> str:
        return "ClassStorageTaskResolver"

    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        return list(self.mapping.keys())

    def add(self, user_function: Callable):
        fn = PythonFunctionTask(task_config=None, task_function=user_function, task_resolver=cls)
        self.mapping[fn] = user_function

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        if len(loader_args) != 1:
            raise RuntimeError(f"Unable to load task, received ambiguous loader args {loader_args}, expected only one")

        # string should be parseable a an int
        print(loader_args[0])
        idx = int(loader_args[0])
        k = list(self.mapping.keys())

        return self.mapping[k[idx]]

    def loader_args(self, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        """
        This is responsible for turning an instance of a task into args that the load_task function can reconstitute.
        """
        if t not in self.mapping:
            raise Exception("no such task")

        return [f"{list(cls.mapping.keys()).index(t)}"]

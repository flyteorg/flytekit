from typing import List, Callable

from flytekit.core.python_auto_container import TaskResolverMixin, PythonAutoContainerTask
from flytekit.core.python_function_task import PythonFunctionTask, PythonInstanceTask
from flytekit.core.workflow import Workflow
from flytekit import workflow, task
from flytekit.core.context_manager import SerializationSettings
from collections import OrderedDict


class ClassStoringMeta(type):
    """
    This is here so that when people inherit from the class below, new dictionaries are created.

    Can just do the whole InstanceVar LHS dance, instead of this too.
    """
    def __new__(mcs, name, bases, dct):
        klass = super().__new__(mcs, name, bases, dct)
        klass.d = {}
        klass.reverse = OrderedDict()
        return klass


# Single task workflow builder
class ClassStorageTaskResolver(TaskResolverMixin, metaclass=ClassStoringMeta):
    """
    Stores tasks inside a class variable. The class must be inherited from at the point of usage because the task
    loading process basically relies on the same sequence of things happening.
    """

    @classmethod
    def name(cls) -> str:
        return "Builder"

    @classmethod
    def get_all_tasks(cls) -> List[PythonAutoContainerTask]:
        return list(cls.reverse.keys())

    @classmethod
    def add(cls, user_function: Callable):
        fn = PythonFunctionTask(task_config=None, task_function=user_function, task_resolver=cls)
        print(user_function)
        cls.d[user_function] = fn
        cls.reverse[fn] = user_function

    @classmethod
    def load_task(cls, loader_args: List[str]) -> PythonAutoContainerTask:
        if len(loader_args) != 1:
            raise RuntimeError(f"Unable to load task, received ambiguous loader args {loader_args}, expected only one")

        # string should be parseable a an int
        print(loader_args[0])
        idx = int(loader_args[0])
        k = list(cls.reverse.keys())

        return cls.reverse[k[idx]]

    @classmethod
    def loader_args(cls, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        """
        This is responsible for turning an instance of a task into args that the load_task function can reconstitute.

        """
        if t not in cls.reverse:
            raise Exception("no such task")

        return [f"{list(cls.reverse.keys()).index(t)}"]

    @classmethod
    def build(cls) -> Workflow:
        @workflow
        def wf_example():
            pass

        return wf_example

import collections
import importlib
import typing
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type

import jsonpickle
from langchain_core.runnables import Runnable

from flytekit import FlyteContextManager, logger
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import SyncAgentExecutorMixin


@dataclass
class LangChainObj(object):
    """
    This class is used to store the LangChain task configuration.
    It is serialized and stored in the Flyte task config.
    Every LangChain task has a module, name and parameters.
    They are all Runnable objects.

    from langchain_core.prompts import ChatPromptTemplate

    prompt = ChatPromptTemplate.from_template("{topic}")

    In this case, the attributes of LangChainObj will be:
    module: langchain_core.output_parsers
    name: StrOutputParser
    parameters: {"topic"}
    """

    module: str
    name: str
    parameters: typing.Dict[str, Any]


class LangChainTask(SyncAgentExecutorMixin, PythonTask[LangChainObj]):
    """
    This python task is used to wrap an LangChain task. It is used to run an LangChain task in Flyte agent.
    The langchain task module, name and parameters are stored in the task config. We run the LangChain task in the agent.
    """

    _TASK_TYPE = "langchain"

    def __init__(
        self,
        name: str,
        task_config: LangChainObj,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        inputs = {"input": Any}
        outputs = {"o0": Any}
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        # Use jsonpickle to serialize the LangChain task config since the return value should be json serializable.
        return {"task_config_pkl": jsonpickle.encode(self.task_config)}


def _get_langchain_instance(
    langchain_obj: LangChainObj,
) -> Any:
    print("langchain_obj:", langchain_obj)
    obj_module = importlib.import_module(name=langchain_obj.module)
    obj_def = getattr(obj_module, langchain_obj.name)
    print("obj_def:", obj_def)
    return obj_def
    print("obj_def:", obj_def)
    return obj_def(**langchain_obj.parameters)


def _flyte_runnable(
    *args,
    **kwargs,
) -> Runnable:
    # TODO: Need to specify task id here, cause every task in flyet should have unique name.
    """
    This function is called by the Flyte task to create a new LangChain task.
    """
    # print(type(args), type(kwargs))
    # print("args:", args)
    # print("args len:", len(args))
    # print("kwargs:", kwargs)
    # print("kwargs len:", len(kwargs))

    cls = args[0]
    task_id = kwargs.get("task_id", cls.__name__)
    config = LangChainObj(module=cls.__module__, name=cls.__name__, parameters=kwargs)
    return LangChainTask(name=task_id, task_config=config)


Runnable.__new__ = _flyte_runnable

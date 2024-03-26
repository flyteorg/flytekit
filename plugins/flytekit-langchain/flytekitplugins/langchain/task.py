import collections
import importlib
import typing
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type

import jsonpickle
from langchain_core.runnables import Runnable

from flytekit import FlyteContextManager
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface


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


# class LangChainTaskResolver(AsyncAgentExecutorMixin, PythonTask):
class LangChainTask(PythonTask[LangChainObj]):
    """
    This python task is used to wrap an LangChain task. It is used to run an LangChain task in Flyte agent.
    The langchain task module, name and parameters are stored in the task config. We run the LangChain task in the agent.
    """

    _TASK_TYPE = "langchain"

    def __init__(
        self,
        name: str,
        task_config: Optional[LangChainObj],
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        inputs = collections.OrderedDict({"input": Any})
        outputs = collections.OrderedDict({"output": Any})
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}, outputs=outputs or {}),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        # Use jsonpickle to serialize the Airflow task config since the return value should be json serializable.
        return {"task_config_pkl": jsonpickle.encode(self.task_config)}

    def execute(self, **kwargs) -> Any:
        print("name:", self.name)
        print("task_config:", self.task_config)
        print("kwargs:", kwargs)
        print("hello world")
        return 
        # print("python task execute:", kwargs)
        return super().execute(**kwargs)


def _get_langchain_instance(
    langchain_obj: LangChainObj,
) -> Any:
    # Set the GET_ORIGINAL_TASK attribute to True so that obj_def will return the original
    # langchain task instead of the Flyte task.
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params.builder().add_attr("GET_ORIGINAL_TASK", True).build()

    obj_module = importlib.import_module(name=langchain_obj.module)
    obj_def = getattr(obj_module, langchain_obj.name)

    return obj_def(**langchain_obj.parameters)


def _flyte_runnable(
    *args,
    **kwargs,
) -> Runnable:
    # TODO: Need to specify task id here, cause every task in flyet should have unique name.
    """
    This function is called by the Flyte task to create a new LangChain task.
    """
    print(type(args), type(kwargs))
    print("args:", args)
    print("args len:", len(args))
    print("kwargs:", kwargs)
    print("kwargs len:", len(kwargs))

    cls = args[0]
    task_id = kwargs.get("task_id", cls.__name__)
    config = LangChainObj(module=cls.__module__, name=cls.__name__, parameters=kwargs)
    # print("task_id:", task_id)
    # print("config", config)
    return LangChainTask(name=task_id, task_config=config)


Runnable.__new__ = _flyte_runnable


# def _flyte_operator(*args, **kwargs):
#     """
#     This function is called by the Airflow operator to create a new task. We intercept this call and return a Flyte
#     task instead.
#     """
#     cls = args[0]
#     try:
#         if FlyteContextManager.current_context().user_space_params.get_original_task:
#             # Return original task when running in the agent.
#             return object.__new__(cls)
#     except AssertionError:
#         # This happens when the task is created in the dynamic workflow.
#         # We don't need to return the original task in this case.
#         logging.debug("failed to get the attribute GET_ORIGINAL_TASK from user space params")

#     container_image = kwargs.pop("container_image", None)
#     task_id = kwargs["task_id"] or cls.__name__
#     config = LangChainObj(module=cls.__module__, name=cls.__name__, parameters=kwargs)

#     return AirflowTask(name=task_id, task_config=config)()


# def _flyte_xcom_push(*args, **kwargs):
#     """
#     This function is called by the Airflow operator to push data to XCom. We intercept this call and store the data
#     in the Flyte context.
#     """
#     if len(args) < 2:
#         return
#     # Store the XCom data in the Flyte context.
#     # args[0] is the operator instance.
#     # args[1:] are the XCom data.
#     # For example,
#     # op.xcom_push(Context(), "key", "value")
#     # args[0] is op, args[1:] is [Context(), "key", "value"]
#     FlyteContextManager.current_context().user_space_params.xcom_data = args[1:]


# params = FlyteContextManager.current_context().user_space_params
# params.builder().add_attr("GET_ORIGINAL_TASK", False).add_attr("XCOM_DATA", {}).build()

# We need this to monkey patch the LangChain Task

# Monkey patch the Airflow operator. Instead of creating an airflow task, it returns a Flyte task.
# airflow_models.BaseOperator.__new__ = _flyte_operator
# airflow_models.BaseOperator.xcom_push = _flyte_xcom_push
# # Monkey patch the xcom_push method to store the data in the Flyte context.
# # Create a dummy DAG to avoid Airflow errors. This DAG is not used.
# # TODO: Add support using Airflow DAG in Flyte workflow. We can probably convert the Airflow DAG to a Flyte subworkflow.
# airflow_sensors.BaseSensorOperator.dag = airflow.DAG(dag_id="flyte_dag")

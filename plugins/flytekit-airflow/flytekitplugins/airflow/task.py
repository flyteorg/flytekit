import importlib
import logging
import typing
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type

import jsonpickle

import airflow
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger
from airflow.utils import context
from flytekit import FlyteContextManager, logger
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask, TaskResolverMixin
from flytekit.core.context_manager import CompilationState
from flytekit.core.interface import Interface
from flytekit.core.python_auto_container import PythonAutoContainerTask
from flytekit.core.tracker import TrackedInstance
from flytekit.core.utils import timeit
from flytekit.core.workflow import (
    PythonFunctionWorkflow,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class AirflowObj(object):
    """
    This class is used to store the Airflow task configuration. It is serialized and stored in the Flyte task config.
    It can be trigger, hook, operator or sensor. For example:

    from airflow.sensors.filesystem import FileSensor
    sensor = FileSensor(task_id="id", filepath="/tmp/1234")

    In this case, the attributes of AirflowObj will be:
    module: airflow.sensors.filesystem
    name: FileSensor
    parameters: {"task_id": "id", "filepath": "/tmp/1234"}
    """

    module: str
    name: str
    parameters: typing.Dict[str, Any]


class AirflowTaskResolver(TrackedInstance, TaskResolverMixin):
    """
    This class is used to resolve an Airflow task. It will load an airflow task in the container.
    """

    def name(self) -> str:
        return "AirflowTaskResolver"

    @timeit("Load airflow task")
    def load_task(self, loader_args: typing.List[str]) -> typing.Union[BaseOperator, BaseSensorOperator, BaseTrigger]:
        """
        This method is used to load an Airflow task.
        """
        _, task_module, _, task_name, _, task_config = loader_args
        task_module = importlib.import_module(name=task_module)  # type: ignore
        task_def = getattr(task_module, task_name)
        return task_def(name=task_name, task_config=jsonpickle.decode(task_config))

    def loader_args(self, settings: SerializationSettings, task: PythonAutoContainerTask) -> typing.List[str]:
        return [
            "task-module",
            task.__module__,
            "task-name",
            task.__class__.__name__,
            "task-config",
            jsonpickle.encode(task.task_config),
        ]

    def get_all_tasks(self) -> typing.List[PythonAutoContainerTask]:  # type: ignore
        raise Exception("should not be needed")


airflow_task_resolver = AirflowTaskResolver()


class AirflowContainerTask(PythonAutoContainerTask[AirflowObj]):
    """
    This python container task is used to wrap an Airflow task. It is used to run an Airflow task in a container.
    The airflow task module, name and parameters are stored in the task config.

    Some of the Airflow operators are not deferrable, For example, BeamRunJavaPipelineOperator, BeamRunPythonPipelineOperator.
    These tasks don't have an async method to get the job status, so cannot be used in the Flyte agent. We run these tasks in a container.
    """

    def __init__(
        self,
        name: str,
        task_config: AirflowObj,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}),
            **kwargs,
        )
        self._task_resolver = airflow_task_resolver

    def execute(self, **kwargs) -> Any:
        logger.info("Executing Airflow task")
        _get_airflow_instance(self.task_config).execute(context=context.Context())


class AirflowTask(AsyncAgentExecutorMixin, PythonTask[AirflowObj]):
    """
    This python task is used to wrap an Airflow task. It is used to run an Airflow task in Flyte agent.
    The airflow task module, name and parameters are stored in the task config. We run the Airflow task in the agent.
    """

    _TASK_TYPE = "airflow"

    def __init__(
        self,
        name: str,
        task_config: Optional[AirflowObj],
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        # Use jsonpickle to serialize the Airflow task config since the return value should be json serializable.
        return {"task_config_pkl": jsonpickle.encode(self.task_config)}


def _get_airflow_instance(airflow_obj: AirflowObj) -> typing.Union[BaseOperator, BaseSensorOperator, BaseTrigger]:
    # Set the GET_ORIGINAL_TASK attribute to True so that obj_def will return the original
    # airflow task instead of the Flyte task.
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params.builder().add_attr("GET_ORIGINAL_TASK", True).build()

    obj_module = importlib.import_module(name=airflow_obj.module)
    obj_def = getattr(obj_module, airflow_obj.name)
    if _is_deferrable(obj_def):
        try:
            return obj_def(**airflow_obj.parameters, deferrable=True)
        except airflow.exceptions.AirflowException as e:
            logger.debug(f"Failed to create operator {airflow_obj.name} with err: {e}.")
            logger.debug(f"Airflow operator {airflow_obj.name} does not support deferring.")

    return obj_def(**airflow_obj.parameters)


def _is_deferrable(cls: Type) -> bool:
    """
    This function is used to check if the Airflow operator is deferrable.
    If the operator is not deferrable, we run it in a container instead of the agent.
    """
    # Only Airflow operators are deferrable.
    if not issubclass(cls, BaseOperator):
        return False
    # Airflow sensors are not deferrable. The Sensor is a subclass of BaseOperator.
    if issubclass(cls, BaseSensorOperator):
        return False
    try:
        from airflow.providers.apache.beam.operators.beam import BeamBasePipelineOperator

        # Dataflow operators are not deferrable.
        if issubclass(cls, BeamBasePipelineOperator):
            return False
    except ImportError:
        logger.debug("Failed to import BeamBasePipelineOperator")
    return True


def _flyte_operator(*args, **kwargs):
    """
    This function is called by the Airflow operator to create a new task. We intercept this call and return a Flyte
    task instead.
    """
    cls = args[0]
    try:
        if FlyteContextManager.current_context().user_space_params.get_original_task:
            # Return an original task when running in the agent.
            return object.__new__(cls)
    except AssertionError:
        # This happens when the task is created in the dynamic workflow.
        # We don't need to return the original task in this case.
        logging.debug("failed to get the attribute GET_ORIGINAL_TASK from user space params")

    container_image = kwargs.pop("container_image", None)
    task_id = kwargs.get("task_id", cls.__name__)
    config = AirflowObj(module=cls.__module__, name=cls.__name__, parameters=kwargs)

    if not issubclass(cls, BaseSensorOperator) and not _is_deferrable(cls):
        # Dataflow operators are not deferrable, so we run them in a container.
        return AirflowContainerTask(name=task_id, task_config=config, container_image=container_image)()
    return AirflowTask(name=task_id, task_config=config)()


class FlyteWorkflow(PythonFunctionWorkflow):
    def __init__(self):
        workflow_metadata = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)
        workflow_metadata_defaults = WorkflowMetadataDefaults(interruptible=False)
        super().__init__(
            workflow_function=lambda _: None, metadata=workflow_metadata, default_metadata=workflow_metadata_defaults
        )

    def __enter__(self):
        ctx = FlyteContextManager.current_context()
        prefix = ctx.compilation_state.prefix if ctx.compilation_state is not None else ""
        FlyteContextManager.push_context(
            ctx.with_compilation_state(CompilationState(prefix=prefix, task_resolver=self)).build()
        )
        print("Entering workflow")

    def __exit__(self, *exc):
        print("Exiting workflow")
        FlyteContextManager.pop_context()


def _flyte_workflow(*args, **kwargs):
    print(args)
    print(kwargs)
    return FlyteWorkflow()


def _flyte_xcom_push(*args, **kwargs):
    """
    This function is called by the Airflow operator to push data to XCom. We intercept this call and store the data
    in the Flyte context.
    """
    if len(args) < 2:
        return
    # Store the XCom data in the Flyte context.
    # args[0] is the operator instance.
    # args[1:] are the XCom data.
    # For example,
    # op.xcom_push(Context(), "key", "value")
    # args[0] is an op, args[1:] is [Context(), "key", "value"]
    FlyteContextManager.current_context().user_space_params.xcom_data = args[1:]


params = FlyteContextManager.current_context().user_space_params
params.builder().add_attr("GET_ORIGINAL_TASK", False).add_attr("XCOM_DATA", {}).build()

# Monkey patch the Airflow operator. Instead of creating an airflow task, it returns a Flyte task.
BaseOperator.__new__ = _flyte_operator
BaseOperator.xcom_push = _flyte_xcom_push
# Monkey patch the xcom_push method to store the data in the Flyte context.
# Create a dummy DAG to avoid Airflow errors. This DAG is not used.
# TODO: Add support using Airflow DAG in Flyte workflow. We can probably convert the Airflow DAG to a Flyte subworkflow.
BaseSensorOperator.dag = airflow.DAG(dag_id="flyte_dag")
airflow.DAG.__new__ = _flyte_workflow

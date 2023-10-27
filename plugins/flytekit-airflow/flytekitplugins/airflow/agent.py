import importlib
import typing
from dataclasses import dataclass, field
from typing import Optional

import cloudpickle
import grpc
import jsonpickle
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource, State,
)

from flytekit.exceptions.user import FlyteUserException
from flytekitplugins.airflow.task import AirflowConfig
from google.cloud.exceptions import NotFound

from flytekit import FlyteContext, FlyteContextManager, logger
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class ResourceMetadata:
    airflow_config: AirflowConfig
    job_id: typing.Optional[str] = field(default=None)
    operation: typing.Optional[Future] = field(default=None)

    def with_job_id(self, job_id: str):
        return ResourceMetadata(airflow_config=self.airflow_config if self.airflow_config else None,
                                job_id=job_id)


def _get_airflow_task(airflow_config: AirflowConfig):
    ctx = FlyteContextManager.current_context()
    task_module = importlib.import_module(name=airflow_config.task_module)
    task_def = getattr(task_module, airflow_config.task_name)
    task_config = airflow_config.task_config

    # Set the GET_ORIGINAL_TASK attribute to True so that task_def will return the original
    # airflow task instead of the Flyte task.
    ctx.user_space_params.builder().add_attr("GET_ORIGINAL_TASK", True).build()
    return task_def(**task_config)


def _execute_airflow_operator(airflow_operator: BaseOperator, airflow_config: AirflowConfig):
    """
    It's
    """
    try:
        from airflow.providers.google.cloud.operators.dataproc import DataprocJobBaseOperator, DataprocDeleteClusterOperator, DataprocHook
        ctx = FlyteContextManager.current_context()
        if isinstance(airflow_operator, DataprocJobBaseOperator):
            airflow_operator.asynchronous = True
            airflow_operator.execute(context=Context())
            job_id = ctx.user_space_params.xcom_data["value"]["resource"]
            return ResourceMetadata(job_id=job_id, airflow_config=airflow_config)
        elif isinstance(airflow_operator, DataprocDeleteClusterOperator):
            hook = DataprocHook(gcp_conn_id=airflow_operator.gcp_conn_id, impersonation_chain=airflow_operator.impersonation_chain)
            operation = hook.delete_cluster()
            operation.done()
            return ResourceMetadata(airflow_config=airflow_config)
    except ImportError:
        logger.debug("apache-airflow[google] is not installed")

    return ResourceMetadata(airflow_config=airflow_config)


def _get_status_from_operator(airflow_operator: BaseOperator, meta: ResourceMetadata) -> State:
    job_id = meta.job_id
    airflow_config = meta.airflow_config
    try:
        from airflow.providers.google.cloud.operators.dataproc import DataprocJobBaseOperator, JobStatus, DataprocDeleteClusterOperator
        if isinstance(airflow_operator, DataprocJobBaseOperator):
            job = airflow_operator.hook.get_job(
                job_id=job_id,
                region=airflow_config.task_config["region"],
                project_id=airflow_config.task_config["project_id"],
            )
            if job.status.state == JobStatus.State.DONE:
                return SUCCEEDED
            elif job.status.state in (JobStatus.State.ERROR, JobStatus.State.CANCELLED):
                return PERMANENT_FAILURE
        elif isinstance(airflow_operator, DataprocDeleteClusterOperator):
            try:
                airflow_operator.execute(context=Context())
            except NotFound:
                logger.info("Dataproc cluster already deleted.")
            return SUCCEEDED


class AirflowAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="airflow", asynchronous=False)

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        airflow_config = jsonpickle.decode(task_template.custom.get("task_config_pkl"))

        airflow_task = _get_airflow_task(airflow_config)
        if isinstance(airflow_task, BaseSensorOperator):
            resource_meta = ResourceMetadata(airflow_config=airflow_config)
        elif isinstance(airflow_task, BaseOperator):
            resource_meta = _execute_airflow_operator(airflow_task, airflow_config)
        else:
            raise FlyteUserException("Airflow task must be a BaseOperator or BaseSensorOperator")

        return CreateTaskResponse(resource_meta=cloudpickle.dumps(resource_meta))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        airflow_task = _get_airflow_task(meta.airflow_config)
        cur_state = RUNNING

        if isinstance(airflow_task, BaseSensorOperator):
            ok = airflow_task.poke(context=Context())
            cur_state = SUCCEEDED if ok else RUNNING
        elif isinstance(airflow_task, BaseSensorOperator):
            meta = cloudpickle.loads(resource_meta)
            _get_status_from_operator()
        else:
            ...
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

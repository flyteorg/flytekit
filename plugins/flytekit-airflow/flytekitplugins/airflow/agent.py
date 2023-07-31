import importlib
from dataclasses import dataclass
from typing import Optional

import cloudpickle
import grpc
import jsonpickle
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
    DataprocJobBaseOperator,
    JobStatus,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from flytekitplugins.airflow.task import AirflowConfig

from flytekit import FlyteContext, FlyteContextManager, logger
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class ResourceMetadata:
    job_id: str
    airflow_config: AirflowConfig


def _get_airflow_task(ctx: FlyteContext, airflow_config: AirflowConfig):
    task_module = importlib.import_module(name=airflow_config.task_module)
    task_def = getattr(task_module, airflow_config.task_name)
    ctx.user_space_params.builder().add_attr("GET_ORIGINAL_TASK", True).build()
    task_config = airflow_config.task_config
    if issubclass(task_def, DataprocJobBaseOperator):
        return task_def(**task_config, asynchronous=True)
    return task_def(**task_config)


class AirflowAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="airflow")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        airflow_config = cloudpickle.loads(jsonpickle.decode(task_template.custom.get("task_config_pkl")))
        resource_meta = ResourceMetadata(job_id="", airflow_config=airflow_config)

        ctx = FlyteContextManager.current_context()
        airflow_task = _get_airflow_task(ctx, airflow_config)
        if isinstance(airflow_task, DataprocJobBaseOperator):
            airflow_task.execute(context=Context())
            resource_meta.job_id = ctx.user_space_params.xcom_data["value"]["resource"]

        return CreateTaskResponse(resource_meta=cloudpickle.dumps(resource_meta))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        airflow_config = meta.airflow_config
        job_id = meta.job_id
        task = _get_airflow_task(FlyteContextManager.current_context(), meta.airflow_config)
        cur_state = RUNNING
        try:
            if issubclass(type(task), BaseSensorOperator):
                if task.poke(context=Context()):
                    cur_state = SUCCEEDED
            elif issubclass(type(task), DataprocJobBaseOperator):
                job = task.hook.get_job(
                    job_id=job_id,
                    region=airflow_config.task_config["region"],
                    project_id=airflow_config.task_config["project_id"],
                )
                if job.status.state == JobStatus.State.DONE:
                    cur_state = SUCCEEDED
                elif job.status.state in (JobStatus.State.ERROR, JobStatus.State.CANCELLED):
                    cur_state = PERMANENT_FAILURE
            else:
                res = task.execute(context=Context())
                if res or (res is None and isinstance(task, DataprocDeleteClusterOperator)):
                    cur_state = SUCCEEDED
        except Exception as e:
            logger.error(e.__str__())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(e.__str__())
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

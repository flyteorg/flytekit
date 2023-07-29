import importlib
from dataclasses import dataclass
from typing import Optional

import cloudpickle
import grpc
from airflow.providers.google.cloud.operators.dataproc import DataprocJobBaseOperator
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

from flytekit import FlyteContextManager, logger
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class ResourceMetadata:
    job_id: str
    airflow_config: AirflowConfig


def _get_airflow_task(airflow_config: AirflowConfig):
    task_module = importlib.import_module(name=airflow_config.task_module)
    task_def = getattr(task_module, airflow_config.task_name)
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params._attrs["GET_ORIGINAL_TASK"] = True
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
        airflow_config = cloudpickle.loads(task_template.custom.get("task_config_pkl"))
        resource_meta = ResourceMetadata(job_id="", airflow_config=airflow_config)
        task = _get_airflow_task(airflow_config)
        if isinstance(task, DataprocJobBaseOperator):
            # TODO: we should read job_id from xcom because task.execute() won't return task ID
            job_id = task.execute(context=Context())
            resource_meta.job_id = job_id

        return CreateTaskResponse(resource_meta=cloudpickle.dumps(resource_meta))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        airflow_config = meta.airflow_config
        job_id = meta.job_id
        task = _get_airflow_task(meta.airflow_config)
        try:
            if issubclass(type(task), BaseSensorOperator):
                res = task.poke(context=Context())
            elif issubclass(type(task), DataprocJobBaseOperator):
                res = task.hook.get_job(
                    job_id=job_id,
                    region=airflow_config.task_config["region"],
                    project_id=airflow_config.task_config["project_id"],
                )
            else:
                res = task.execute(context=Context())
            if res:
                cur_state = SUCCEEDED
            else:
                cur_state = RUNNING
        except Exception as e:
            logger.error(e.__str__())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(e.__str__())
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        # Do Nothing
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

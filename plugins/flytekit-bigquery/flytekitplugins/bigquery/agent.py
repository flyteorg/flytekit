import datetime
import json
from dataclasses import asdict, dataclass
from typing import Dict, Optional

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from google.cloud import bigquery

from flytekit import FlyteContextManager, StructuredDataset, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType

pythonTypeToBigQueryType: Dict[type, str] = {
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#data_type_sizes
    list: "ARRAY",
    bool: "BOOL",
    bytes: "BYTES",
    datetime.datetime: "DATETIME",
    float: "FLOAT64",
    int: "INT64",
    str: "STRING",
}


@dataclass
class Metadata:
    job_id: str
    project: str
    location: str


class BigQueryAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="bigquery_query_job_task")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        job_config = None
        if inputs:
            ctx = FlyteContextManager.current_context()
            python_interface_inputs = {
                name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
            }
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            logger.info(f"Create BigQuery job config with inputs: {native_inputs}")
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(name, pythonTypeToBigQueryType[python_interface_inputs[name]], val)
                    for name, val in native_inputs.items()
                ]
            )

        custom = task_template.custom
        project = custom["ProjectID"]
        location = custom["Location"]
        client = bigquery.Client(project=project, location=location)
        query_job = client.query(task_template.sql.statement, job_config=job_config)
        metadata = Metadata(job_id=str(query_job.job_id), location=location, project=project)

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        client = bigquery.Client()
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        job = client.get_job(metadata.job_id, metadata.project, metadata.location)
        if job.errors:
            logger.error(job.errors.__str__())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(job.errors.__str__())
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
        cur_state = convert_to_flyte_state(str(job.state))
        res = None

        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            if job.destination:
                output_location = (
                    f"bq://{job.destination.project}:{job.destination.dataset_id}.{job.destination.table_id}"
                )
                res = literals.LiteralMap(
                    {
                        "results": TypeEngine.to_literal(
                            ctx,
                            StructuredDataset(uri=output_location),
                            StructuredDataset,
                            LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                        )
                    }
                ).to_flyte_idl()

        return GetTaskResponse(resource=Resource(state=cur_state, outputs=res))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        client = bigquery.Client()
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        client.cancel_job(metadata.job_id, metadata.project, metadata.location)
        return DeleteTaskResponse()


AgentRegistry.register(BigQueryAgent())

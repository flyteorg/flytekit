import datetime
from typing import Dict, Optional

import grpc
from flyteidl.service.external_plugin_service_pb2 import (
    SUCCEEDED,
    TaskCreateResponse,
    TaskDeleteResponse,
    TaskGetResponse,
)
from google.cloud import bigquery

from flytekit import FlyteContextManager, StructuredDataset, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry, convert_to_flyte_state
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


class BigQueryPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="bigquery_query_job_task")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> TaskCreateResponse:
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
        client = bigquery.Client(project=custom["ProjectID"], location=custom["Location"])
        query_job = client.query(task_template.sql.statement, job_config=job_config)

        return TaskCreateResponse(job_id=str(query_job.job_id))

    def get(self, context: grpc.ServicerContext, job_id: str) -> TaskGetResponse:
        client = bigquery.Client()
        job = client.get_job(job_id)
        cur_state = convert_to_flyte_state(str(job.state))
        res = None

        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            output_location = f"bq://{job.destination.project}:{job.destination.dataset_id}.{job.destination.table_id}"
            res = literals.LiteralMap(
                {
                    "results": TypeEngine.to_literal(
                        ctx,
                        StructuredDataset(uri=output_location),
                        StructuredDataset,
                        LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                    )
                }
            )

        return TaskGetResponse(state=cur_state, outputs=res.to_flyte_idl())

    def delete(self, context: grpc.ServicerContext, job_id: str) -> TaskDeleteResponse:
        client = bigquery.Client()
        client.cancel_job(job_id)
        return TaskDeleteResponse()


BackendPluginRegistry.register(BigQueryPlugin())

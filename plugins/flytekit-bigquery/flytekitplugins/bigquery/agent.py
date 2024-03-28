import datetime
from dataclasses import dataclass
from typing import Dict, Optional

from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from google.cloud import bigquery

from flytekit import FlyteContextManager, StructuredDataset, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

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
class BigQueryMetadata(ResourceMeta):
    job_id: str
    project: str
    location: str


class BigQueryAgent(AsyncAgentBase):
    name = "Bigquery Agent"

    def __init__(self):
        super().__init__(task_type_name="bigquery_query_job_task", metadata_type=BigQueryMetadata)

    def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> BigQueryMetadata:
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

        return BigQueryMetadata(job_id=str(query_job.job_id), location=location, project=project)

    def get(self, resource_meta: BigQueryMetadata, **kwargs) -> Resource:
        client = bigquery.Client()
        log_link = TaskLog(
            uri=f"https://console.cloud.google.com/bigquery?project={resource_meta.project}&j=bq:{resource_meta.location}:{resource_meta.job_id}&page=queryresults",
            name="BigQuery Console",
        )

        job = client.get_job(resource_meta.job_id, resource_meta.project, resource_meta.location)
        if job.errors:
            logger.error("failed to run BigQuery job with error:", job.errors.__str__())
            return Resource(phase=TaskExecution.FAILED, message=job.errors.__str__(), log_links=[log_link])

        cur_phase = convert_to_flyte_phase(str(job.state))
        res = None

        if cur_phase == TaskExecution.SUCCEEDED:
            dst = job.destination
            if dst:
                ctx = FlyteContextManager.current_context()
                output_location = f"bq://{dst.project}:{dst.dataset_id}.{dst.table_id}"
                res = TypeEngine.dict_to_literal_map(ctx, {"results": StructuredDataset(uri=output_location)})

        return Resource(phase=cur_phase, message=str(job.state), log_links=[log_link], outputs=res)

    def delete(self, resource_meta: BigQueryMetadata, **kwargs):
        client = bigquery.Client()
        client.cancel_job(resource_meta.job_id, resource_meta.project, resource_meta.location)


AgentRegistry.register(BigQueryAgent())

from google.cloud import bigquery

from flytekit.extend.backend.base_plugin import (
    BackendPluginBase,
    BackendPluginRegistry,
    CreateRequest,
    CreateResponse,
    PollResponse,
    convert_to_flyte_state,
)


class BigQueryPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="bigquery", version="v1")

    async def initialize(self):
        return "Hello World"

    async def create(self, create_request: CreateRequest) -> CreateResponse:
        client = bigquery.Client()
        QUERY = "SELECT 1"
        query_job = client.query(QUERY)
        return CreateResponse(job_id=query_job.job_id)

    async def poll(self, job_id) -> PollResponse:
        client = bigquery.Client()
        job = client.get_job(job_id)
        return PollResponse(job_id=job.job_id, state=convert_to_flyte_state(job.state))

    async def terminate(self, job_id):
        client = bigquery.Client()
        client.cancel_job(job_id)


BackendPluginRegistry.register(BigQueryPlugin())

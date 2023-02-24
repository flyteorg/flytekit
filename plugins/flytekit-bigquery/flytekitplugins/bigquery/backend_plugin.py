from typing import Dict

from google.cloud import bigquery

from flytekit import FlyteContextManager, StructuredDataset
from flytekit.core import constants
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_plugin import (
    SUCCEEDED,
    BackendPluginBase,
    BackendPluginRegistry,
    CreateRequest,
    CreateResponse,
    PollRequest,
    PollResponse,
    convert_to_flyte_state,
)
from flytekit.extend.backend.utils import get_task_inputs, get_task_template, upload_output_file
from flytekit.models import literals
from flytekit.models.types import LiteralType, StructuredDatasetType

pythonTypeToBigQueryType: Dict[type, str] = {
    str: "STRING",
    int: "INT64",
}


class BigQueryPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="bigquery")

    def initialize(self):
        # TODO: Read GOOGLE_APPLICATION_CREDENTIALS from secret. If not found, raise an error.
        pass

    def create(self, create_request: CreateRequest) -> CreateResponse:
        ctx = FlyteContextManager.current_context()
        task_template = get_task_template(create_request.task_template_path)
        task_input_literals = get_task_inputs(create_request.inputs_path)

        # 3. Submit the job
        # TODO: is there any other way to get python interface input?
        python_interface_inputs = {
            name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
        }
        native_inputs = TypeEngine.literal_map_to_kwargs(ctx, task_input_literals, python_interface_inputs)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(name, pythonTypeToBigQueryType[python_interface_inputs[name]], val)
                for name, val in native_inputs.items()
            ]
        )

        custom = task_template.custom
        client = bigquery.Client(project=custom["ProjectID"], location=custom["Location"])
        query_job = client.query(task_template.sql.statement, job_config=job_config)

        return CreateResponse(job_id=query_job.job_id)

    def poll(self, poll_request: PollRequest) -> PollResponse:
        client = bigquery.Client()
        job = client.get_job(poll_request.job_id)
        state = convert_to_flyte_state(str(job.state))

        if poll_request.prev_state != SUCCEEDED and state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            output_location = f"bq://{job.destination.project}:{job.destination.dataset_id}.{job.destination.table_id}"
            output_file_dict = {
                constants.OUTPUT_FILE_NAME: literals.LiteralMap(
                    {
                        "results": TypeEngine.to_literal(
                            ctx,
                            StructuredDataset(uri=output_location),
                            StructuredDataset,
                            LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                        )
                    }
                )
            }
            upload_output_file(output_file_dict, poll_request.output_prefix)

        return PollResponse(job_id=job.job_id, state=state)

    def terminate(self, job_id):
        client = bigquery.Client()
        client.cancel_job(job_id)


BackendPluginRegistry.register(BigQueryPlugin())

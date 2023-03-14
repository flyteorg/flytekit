from typing import Dict, Optional

from flyteidl.service import plugin_system_pb2
from flyteidl.service.plugin_system_pb2 import TaskGetResponse
from google.cloud import bigquery

from flytekit import FlyteContextManager, StructuredDataset
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry, convert_to_flyte_state
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType

pythonTypeToBigQueryType: Dict[type, str] = {
    str: "STRING",
    int: "INT64",
}


class BigQueryPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="bigquery")

    def create(
        self, inputs: Optional[LiteralMap], output_prefix: str, task_template: TaskTemplate
    ) -> plugin_system_pb2.TaskCreateResponse:
        ctx = FlyteContextManager.current_context()
        python_interface_inputs = {
            name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
        }
        native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(name, pythonTypeToBigQueryType[python_interface_inputs[name]], val)
                for name, val in native_inputs.items()
            ]
        )

        custom = task_template.custom
        client = bigquery.Client(project=custom["ProjectID"], location=custom["Location"])
        query_job = client.query(task_template.sql.statement, job_config=job_config)

        return plugin_system_pb2.TaskCreateResponse(job_id=query_job.job_id)

    def get(self, job_id: str, prev_state: plugin_system_pb2.State) -> plugin_system_pb2.TaskGetResponse:
        if prev_state == plugin_system_pb2.SUCCEEDED:
            return TaskGetResponse(state=plugin_system_pb2.SUCCEEDED)

        client = bigquery.Client()
        job = client.get_job(job_id)
        cur_state = convert_to_flyte_state(str(job.state))
        res = None

        if cur_state == plugin_system_pb2.SUCCEEDED:
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

        return TaskGetResponse(state=cur_state, outputs=res)

    def delete(self, job_id: str) -> plugin_system_pb2.TaskDeleteResponse:
        client = bigquery.Client()
        client.cancel_job(job_id)
        return plugin_system_pb2.TaskDeleteResponse()


BackendPluginRegistry.register(BigQueryPlugin())

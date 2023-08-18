import datetime
import json
from dataclasses import asdict, dataclass
from typing import Dict, Optional

import snowflake.connector
from snowflake.connector import ProgrammingError

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)

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
    query_id: int


class SnowflakeAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="snowflake")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        params = None
        if inputs:
            ctx = FlyteContextManager.current_context()
            python_interface_inputs = {
                name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
            }
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            logger.info(f"Create Snowflake params with inputs: {native_inputs}")
            params = native_inputs

        config = task_template.config
        self.conn = snowflake.connector.connect(
            user=config["user"],
            password=config["password"],
            account=config["account"],
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"]
        )

        self.cs = self.conn.cursor()
        self.cs.execute_async(task_template.sql.statement, params=params)
        metadata = Metadata(query_id=self.cs.sfqid)

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        try:
            query_status = self.conn.get_query_status_throw_if_error(metadata.query_id)
        except ProgrammingError as err:
            logger.error(err.msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(err.msg)
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
        cur_state = convert_to_flyte_state(str(query_status.name))
        res = None

        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            self.cs.get_results_from_sfqid(metadata.query_id)
            res = literals.LiteralMap(
                {
                    "results": TypeEngine.to_literal(
                        ctx,
                        StructuredDataset(dataframe=self.cs.fetch_pandas_all()),
                        StructuredDataset,
                        LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                    )
                }
            ).to_flyte_idl()
            print(res)

        return GetTaskResponse(resource=Resource(state=cur_state, outputs=res))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        try:
            self.cs.execute(f"SELECT SYSTEM$CANCEL_QUERY('{metadata.query_id}')")
            self.cs.fetchall()
        finally:
            self.cs.close()
        return DeleteTaskResponse()


AgentRegistry.register(SnowflakeAgent())

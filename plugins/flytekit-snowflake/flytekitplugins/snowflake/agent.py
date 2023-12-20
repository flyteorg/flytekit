import json
from dataclasses import asdict, dataclass
from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)

from flytekit import FlyteContextManager, StructuredDataset, lazy_module, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType

snowflake_connector = lazy_module("snowflake.connector")

TASK_TYPE = "snowflake"
SNOWFLAKE_PRIVATE_KEY = "snowflake_private_key"


@dataclass
class Metadata:
    user: str
    account: str
    database: str
    schema: str
    warehouse: str
    table: str
    query_id: str


class SnowflakeAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type=TASK_TYPE)

    def get_private_key(self):
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        import flytekit

        pk_string = flytekit.current_context().secrets.get(SNOWFLAKE_PRIVATE_KEY, encode_mode="rb")
        p_key = serialization.load_pem_private_key(pk_string, password=None, backend=default_backend())

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return pkb

    def get_connection(self, metadata: Metadata) -> snowflake_connector:
        return snowflake_connector.connect(
            user=metadata.user,
            account=metadata.account,
            private_key=self.get_private_key(),
            database=metadata.database,
            schema=metadata.schema,
            warehouse=metadata.warehouse,
        )

    async def async_create(
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
            logger.info(f"Create Snowflake agent params with inputs: {native_inputs}")
            params = native_inputs

        config = task_template.config

        conn = snowflake_connector.connect(
            user=config["user"],
            account=config["account"],
            private_key=self.get_private_key(),
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"],
        )

        cs = conn.cursor()
        cs.execute_async(task_template.sql.statement, params=params)

        metadata = Metadata(
            user=config["user"],
            account=config["account"],
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"],
            table=config["table"],
            query_id=str(cs.sfqid),
        )

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        conn = self.get_connection(metadata)
        try:
            query_status = conn.get_query_status_throw_if_error(metadata.query_id)
        except snowflake_connector.ProgrammingError as err:
            logger.error(err.msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(err.msg)
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
        cur_state = convert_to_flyte_state(str(query_status.name))
        res = None

        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            output_metadata = f"snowflake://{metadata.user}:{metadata.account}/{metadata.warehouse}/{metadata.database}/{metadata.schema}/{metadata.table}"
            res = literals.LiteralMap(
                {
                    "results": TypeEngine.to_literal(
                        ctx,
                        StructuredDataset(uri=output_metadata),
                        StructuredDataset,
                        LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                    )
                }
            ).to_flyte_idl()

        return GetTaskResponse(resource=Resource(state=cur_state, outputs=res))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        conn = self.get_connection(metadata)
        cs = conn.cursor()
        try:
            cs.execute(f"SELECT SYSTEM$CANCEL_QUERY('{metadata.query_id}')")
            cs.fetchall()
        finally:
            cs.close()
            conn.close()
        return DeleteTaskResponse()


AgentRegistry.register(SnowflakeAgent())

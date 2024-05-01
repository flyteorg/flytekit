from dataclasses import dataclass
from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import FlyteContextManager, StructuredDataset, lazy_module, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType

snowflake_connector = lazy_module("snowflake.connector")

TASK_TYPE = "snowflake"
SNOWFLAKE_PRIVATE_KEY = "snowflake_private_key"


@dataclass
class SnowflakeJobMetadata(ResourceMeta):
    user: str
    account: str
    database: str
    schema: str
    warehouse: str
    table: str
    query_id: str


def get_private_key():
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


def get_connection(metadata: SnowflakeJobMetadata) -> snowflake_connector:
    return snowflake_connector.connect(
        user=metadata.user,
        account=metadata.account,
        private_key=get_private_key(),
        database=metadata.database,
        schema=metadata.schema,
        warehouse=metadata.warehouse,
    )


class SnowflakeAgent(AsyncAgentBase):
    name = "Snowflake Agent"

    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SnowflakeJobMetadata)

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SnowflakeJobMetadata:
        ctx = FlyteContextManager.current_context()
        literal_types = task_template.interface.inputs
        params = TypeEngine.literal_map_to_kwargs(ctx, inputs, literal_types=literal_types) if inputs else None

        config = task_template.config
        conn = snowflake_connector.connect(
            user=config["user"],
            account=config["account"],
            private_key=get_private_key(),
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"],
        )

        cs = conn.cursor()
        cs.execute_async(task_template.sql.statement, params=params)

        return SnowflakeJobMetadata(
            user=config["user"],
            account=config["account"],
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"],
            table=config["table"],
            query_id=str(cs.sfqid),
        )

    async def get(self, resource_meta: SnowflakeJobMetadata, **kwargs) -> Resource:
        conn = get_connection(resource_meta)
        try:
            query_status = conn.get_query_status_throw_if_error(resource_meta.query_id)
        except snowflake_connector.ProgrammingError as err:
            logger.error("Failed to get snowflake job status with error:", err.msg)
            return Resource(phase=TaskExecution.FAILED)
        cur_phase = convert_to_flyte_phase(str(query_status.name))
        res = None

        if cur_phase == TaskExecution.SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            output_metadata = f"snowflake://{resource_meta.user}:{resource_meta.account}/{resource_meta.warehouse}/{resource_meta.database}/{resource_meta.schema}/{resource_meta.table}"
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

        return Resource(phase=cur_phase, outputs=res)

    async def delete(self, resource_meta: SnowflakeJobMetadata, **kwargs):
        conn = get_connection(resource_meta)
        cs = conn.cursor()
        try:
            cs.execute(f"SELECT SYSTEM$CANCEL_QUERY('{resource_meta.query_id}')")
            cs.fetchall()
        finally:
            cs.close()
            conn.close()


AgentRegistry.register(SnowflakeAgent())

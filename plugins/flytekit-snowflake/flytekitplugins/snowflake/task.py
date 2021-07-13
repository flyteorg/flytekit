import typing
from dataclasses import dataclass

from dataclasses_json import dataclass_json
from snowflake import connector

from flytekit import current_context, kwtypes
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_customized_container_task import PythonCustomizedContainerTask
from flytekit.core.shim_task import ShimTaskExecutor
from flytekit.models import task as task_models
from flytekit.types.schema import FlyteSchema


@dataclass_json
@dataclass
class SnowflakeConfig(object):
    """
    Use this configuration to configure task.

    params:
        account_name: subdomain for the account.
        username: Username to login as.
        password: The Secret containing the password to login with.
    """
    account_name: str
    username: str
    password: typing.Dict[str, str]
    db: str
    schema: str
    warehouse: str


class SnowflakeTask(PythonCustomizedContainerTask[SnowflakeConfig], SQLTask[SnowflakeConfig]):
    """
    Makes it possible to run client side Snowflake queries that optionally return a FlyteSchema object
    """

    _TASK_TYPE = "snowflake"

    def __init__(
        self,
        name: str,
        query_template: str,
        task_config: SnowflakeConfig,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        output_schema_type: typing.Optional[typing.Type[FlyteSchema]] = None,
        **kwargs,
    ):
        output_schema = output_schema_type if output_schema_type else FlyteSchema
        outputs = kwtypes(results=output_schema)

        super().__init__(
            name=name,
            task_config=task_config,
            container_image="ghcr.io/flyteorg/flytekit:snowflake-test-123",
            executor_type=SnowflakeTaskExecutor,
            task_type=self._TASK_TYPE,
            query_template=query_template,
            inputs=inputs,
            outputs=outputs,
            **kwargs,
        )

    @property
    def output_columns(self) -> typing.Optional[typing.List[str]]:
        c = self.python_interface.outputs["results"].column_names()
        return c if c else None

    def get_custom(self, settings: SerializationSettings) -> typing.Dict[str, typing.Any]:
        return {
            "query_template": self.query_template,
            "config": self.task_config.to_json(),
        }


class SnowflakeTaskExecutor(ShimTaskExecutor[SnowflakeTask]):
    def execute_from_model(self, tt: task_models.TaskTemplate, **kwargs) -> typing.Any:
        if tt.custom["config"] is None:
            raise AssertionError("config must be set")

        secret = tt.custom["config"]["password"]
        secret_value = current_context().secrets.get(secret["group"], secret["key"])

        ctx = connector.connect(
            user=tt.custom["config"]["username"],
            password=secret_value,
            account=tt.custom["config"]["account_name"],
            warehouse=tt.custom["config"]["warehouse"],
            database=tt.custom["config"]["db"],
            schema=tt.custom["config"]["schema"],
            protocol='https')

        # Create a cursor object.
        cur = ctx.cursor()

        # Substitute inputs into query.
        interpolated_query = SnowflakeTask.interpolate_query(tt.custom["query_template"], **kwargs)
        print(f"Interpolated query {interpolated_query}")

        # Execute a statement that will generate a result set.
        cur.execute(interpolated_query)

        # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
        return cur.fetch_pandas_all()

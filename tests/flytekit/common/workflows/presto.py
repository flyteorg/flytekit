from __future__ import absolute_import

from flytekit.common.tasks.presto_task import SdkPrestoTask
from flytekit.sdk.tasks import inputs
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, Output, workflow_class

schema = Types.Schema([("a", Types.String), ("b", Types.Integer)])

presto_task = SdkPrestoTask(
    task_inputs=inputs(ds=Types.String, rg=Types.String),
    statement="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds}}' LIMIT 10",
    output_schema=schema,
    routing_group="{{ .Inputs.rg }}",
    # catalog="hive",
    # schema="city",
)


@workflow_class()
class PrestoWorkflow(object):
    ds = Input(Types.String, required=True, help="Test string with no default")
    # routing_group = Input(Types.String, required=True, help="Test string with no default")

    p_task = presto_task(ds=ds, rg="etl")

    output_a = Output(p_task.outputs.results, sdk_type=schema)

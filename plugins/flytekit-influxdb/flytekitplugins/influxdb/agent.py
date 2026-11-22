from datetime import datetime
from typing import Dict, List, Optional

from flyteidl.core.execution_pb2 import TaskExecution
from influxdb_client import InfluxDBClient
from .utils import DATETIME_FORMAT, Aggregation

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, Resource, SyncAgentBase
from flytekit.extend.backend.utils import get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

INFLUX_API_KEY = "MY_INFLUX_API_KEY"


class InfluxDBAgent(SyncAgentBase):
    name = "InfluxDB Agent"

    def __init__(self):
        super().__init__(task_type_name="influxdb")

    async def do(self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs) -> Resource:
        ctx = FlyteContextManager.current_context()
        input_python_value = TypeEngine.literal_map_to_kwargs(
            ctx,
            inputs,
            {
                "bucket": str,
                "measurement": str,
                "start_time": datetime,
                "end_time": datetime,
                "fields": List[str],
                "tag_dict": Dict[str, List[str]],
                "period_min": int,
                "aggregation": Aggregation,
            },
        )

        custom = task_template.custom
        client = InfluxDBClient(
            url=custom["url"],
            token=get_agent_secret(secret_key=INFLUX_API_KEY),
            org=custom["org"],
            verify_ssl=True
        )

        bucket = input_python_value["bucket"]
        measurement = input_python_value["measurement"]
        start_time = input_python_value["start_time"]
        end_time = input_python_value["end_time"]
        fields = input_python_value["fields"]
        tag_dict = input_python_value["tag_dict"]
        period_min = input_python_value["period_min"]
        aggregation = input_python_value["aggregation"]

        query = InfluxDBAgent.format_query(
            bucket=bucket,
            measurement=measurement,
            start_time=start_time,
            end_time=end_time,
            fields=fields,
            tag_dict=tag_dict,
            period_min=period_min,
            aggregation=aggregation,
        )

        df = client.query_api().query_data_frame(query)
        outputs = {"o0": df.to_json()}

        return Resource(phase=TaskExecution.SUCCEEDED, outputs=outputs)

    @staticmethod
    def format_query(
        bucket: str,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        fields: List[str] = None,
        tag_dict: Dict[str, List[str]] = None,
        period_min: int = None,
        aggregation: Aggregation = None,
    ) -> str:
        start_time_str = start_time.strftime(DATETIME_FORMAT)
        end_time_str = end_time.strftime(DATETIME_FORMAT)
        query = (
            f'from(bucket:"{bucket}")\n'
            f"|> range(start: {start_time_str},stop: {end_time_str} )\n"
            f'|> filter(fn: (r) => r._measurement == "{measurement}")\n'
        )

        if fields is not None:
            filed_dict = {"_field": fields}
            query += InfluxDBAgent.make_query_filter(filed_dict)

        if tag_dict is not None:
            query += InfluxDBAgent.make_query_filter(tag_dict)

        if period_min is not None and aggregation is not None:
            aggregation = f"|> aggregateWindow(every: {period_min}m, fn: {aggregation.value}, " f"createEmpty: true)\n "
            query += aggregation

        query += '  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'

        return query

    @staticmethod
    def make_query_filter(filter_dict: dict) -> str:
        filter_list = []
        for key, value in filter_dict.items():
            value = [value] if type(value) != list else value
            single_filter = " or ".join([f'r.{key} == "{v}"' for v in value])
            filter_list.append(single_filter)

        filters = " and ".join(filter_list)
        return f"|> filter(fn: (r) => {filters})\n"


AgentRegistry.register(InfluxDBAgent())

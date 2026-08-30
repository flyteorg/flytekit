import unittest
from datetime import datetime, timedelta
from io import StringIO
from unittest import mock

import pandas as pd
from pandas._testing import assert_frame_equal

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.literals import LiteralMap
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

dummy_data = {
    "result": ["_result", "_result"],
    "table": [0, 0],
    "_start": ["2024-04-25 00:00:00+00:00", "2024-04-25 00:00:00+00:00"],
    "_stop": ["2024-04-25 00:00:30+00:00", "2024-04-25 00:00:30+00:00"],
    "_time": ["2024-04-25 00:00:00+00:00", "2024-04-25 00:00:30+00:00"],
    "_measurement": ["testing_measurement", "testing_measurement"],
    "testing_tag_key": ["testing_tag_value", "testing_tag_value"],
    "testing_field": [10.5, 20.3],
}


class TestInfluxDBClient(unittest.TestCase):
    @mock.patch("influxdb_client.client.query_api.QueryApi.query_data_frame")
    def test_get_data_from_influxdb(self, mock_dataframe):
        mock_dataframe.return_value = pd.DataFrame(dummy_data)

        agent = AgentRegistry.get_agent("influxdb")
        task_id = Identifier(
            resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
        )
        task_config = {
            "url": "http://dummyurl:8086",
            "org": "my_testing_org",
        }
        task_metadata = TaskMetadata(
            True,
            RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
            timedelta(days=1),
            literals.RetryStrategy(3),
            True,
            "0.1.1b0",
            "This is deprecated!",
            True,
            "A",
            (),
        )
        tmp = TaskTemplate(
            id=task_id,
            custom=task_config,
            metadata=task_metadata,
            interface=None,
            type="influxdb",
        )

        task_inputs = LiteralMap(
            {
                "bucket": literals.Literal(
                    scalar=literals.Scalar(primitive=literals.Primitive(string_value="testing_bucket"))
                ),
                "measurement": literals.Literal(
                    scalar=literals.Scalar(primitive=literals.Primitive(string_value="testing_measurement"))
                ),
                "start_time": literals.Literal(
                    scalar=literals.Scalar(primitive=literals.Primitive(datetime=datetime(2024, 4, 25, 0, 0, 0)))
                ),
                "end_time": literals.Literal(
                    scalar=literals.Scalar(primitive=literals.Primitive(datetime=datetime(2024, 4, 26, 0, 0, 30)))
                ),
                "fields": literals.Literal(
                    collection=literals.LiteralCollection(
                        literals=[
                            literals.Literal(
                                scalar=literals.Scalar(primitive=literals.Primitive(string_value="testing_field"))
                            )
                        ]
                    )
                ),
                "tag_dict": literals.Literal(
                    map=literals.LiteralMap(
                        literals={
                            "testing_tag_key": literals.Literal(
                                collection=literals.LiteralCollection(
                                    literals=[
                                        literals.Literal(
                                            scalar=literals.Scalar(
                                                primitive=literals.Primitive(string_value="testing_tag_value")
                                            )
                                        )
                                    ]
                                )
                            )
                        }
                    )
                ),
                "period_min": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=30))),
                "aggregation": literals.Literal(
                    scalar=literals.Scalar(primitive=literals.Primitive(string_value="last"))
                ),
            },
        )

        response = agent.do(tmp, task_inputs)
        df = pd.read_json(StringIO(response.outputs["o0"]))
        assert_frame_equal(df, pd.DataFrame(dummy_data))

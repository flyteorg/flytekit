from collections import OrderedDict

from flytekitplugins.influxdb import InfluxDBTask

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.models.types import SimpleType


def test_chatgpt_task():
    influx_query_task = InfluxDBTask(
        name="influx",
        url="http://dummyurl:8086",
        org="my_testing_org",
    )

    assert len(influx_query_task.interface.inputs) == 8
    assert len(influx_query_task.interface.outputs) == 1

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    influx_task_spec = get_serializable(OrderedDict(), serialization_settings, influx_query_task)
    custom = influx_task_spec.template.custom
    assert custom["url"] == "http://dummyurl:8086"
    assert custom["org"] == "my_testing_org"

    assert len(influx_task_spec.template.interface.inputs) == 8
    assert len(influx_task_spec.template.interface.outputs) == 1

    assert influx_task_spec.template.interface.inputs["bucket"].type.simple == SimpleType.STRING
    assert influx_task_spec.template.interface.inputs["measurement"].type.simple == SimpleType.STRING
    assert influx_task_spec.template.interface.inputs["start_time"].type.simple == SimpleType.DATETIME
    assert influx_task_spec.template.interface.inputs["end_time"].type.simple == SimpleType.DATETIME
    assert influx_task_spec.template.interface.inputs["fields"].type.collection_type.simple == SimpleType.STRING
    assert (
        influx_task_spec.template.interface.inputs["tag_dict"].type.map_value_type.collection_type.simple
        == SimpleType.STRING
    )
    assert influx_task_spec.template.interface.inputs["period_min"].type.simple == SimpleType.INTEGER
    assert influx_task_spec.template.interface.inputs["aggregation"].type.enum_type.values == [
        "first",
        "last",
        "mean",
        "mode",
    ]

    assert influx_task_spec.template.interface.outputs["o0"].type.simple == SimpleType.STRING

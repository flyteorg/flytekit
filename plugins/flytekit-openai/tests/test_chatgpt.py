from collections import OrderedDict

from flytekitplugins.openai import ChatGPTTask

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.models.types import SimpleType


def test_chatgpt_task():
    chatgpt_task = ChatGPTTask(
        name="chatgpt",
        openai_organization="TEST ORGANIZATION ID",
        chatgpt_config={
            "model": "gpt-3.5-turbo",
            "temperature": 0.7,
        },
    )

    assert len(chatgpt_task.interface.inputs) == 1
    assert len(chatgpt_task.interface.outputs) == 1

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    chatgpt_task_spec = get_serializable(OrderedDict(), serialization_settings, chatgpt_task)
    custom = chatgpt_task_spec.template.custom
    assert custom["openai_organization"] == "TEST ORGANIZATION ID"
    assert custom["chatgpt_config"]["model"] == "gpt-3.5-turbo"
    assert custom["chatgpt_config"]["temperature"] == 0.7

    assert len(chatgpt_task_spec.template.interface.inputs) == 1
    assert len(chatgpt_task_spec.template.interface.outputs) == 1

    assert chatgpt_task_spec.template.interface.inputs["message"].type.simple == SimpleType.STRING
    assert chatgpt_task_spec.template.interface.outputs["o0"].type.simple == SimpleType.STRING

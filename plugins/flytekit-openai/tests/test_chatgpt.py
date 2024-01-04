from collections import OrderedDict

from flytekitplugins.chatgpt import ChatGPTConfig, ChatGPTTask

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable


def test_chatgpt_task_and_config():
    chatgpt_task = ChatGPTTask(
        name="chatgpt",
        task_config=ChatGPTConfig(
        openai_organization="TEST ORGANIZATION ID",
        chatgpt_config={
                    "model": "gpt-3.5-turbo",
                    "temperature": 0.7,
            },
        ),
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

    task_spec = get_serializable(OrderedDict(), serialization_settings, chatgpt_task)
    custom = task_spec.template.custom
    assert custom["openai_organization"] == "TEST ORGANIZATION ID"
    assert custom["chatgpt_config"]["model"] == "gpt-3.5-turbo"
    assert custom["chatgpt_config"]["temperature"] == 0.7

    assert len(task_spec.template.interface.inputs) == 1
    assert len(task_spec.template.interface.outputs) == 1
    
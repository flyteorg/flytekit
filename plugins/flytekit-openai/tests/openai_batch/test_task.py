from collections import OrderedDict
from unittest import mock

from flytekitplugins.openai import BatchEndpointTask, download_files, upload_jsonl_file
from openai.types import FileObject

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.models.types import SimpleType


def test_openai_batch_endpoint_task():
    batch_endpoint_task = BatchEndpointTask(
        name="gpt-3.5-turbo",
        openai_organization="testorg",
        config={"completion_window": "24h"},
    )

    assert len(batch_endpoint_task.interface.inputs) == 1
    assert len(batch_endpoint_task.interface.outputs) == 1

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    batch_endpoint_task_spec = get_serializable(OrderedDict(), serialization_settings, batch_endpoint_task)
    custom = batch_endpoint_task_spec.template.custom

    assert custom["openai_organization"] == "testorg"
    assert custom["config"] == {"completion_window": "24h"}

    assert batch_endpoint_task_spec.template.interface.inputs["input_file_id"].type.simple == SimpleType.STRING
    assert batch_endpoint_task_spec.template.interface.outputs["result"].type.simple == SimpleType.STRING


@mock.patch(
    "openai.files.Files.create",
    return_value=FileObject(
        id="file-abc123",
        object="file",
        bytes=120000,
        created_at=1677610602,
        filename="mydata.jsonl",
        purpose="fine-tune",
    ),
)
def test_upload_jsonl_files_task():
    def jsons():
        for x in [
            {
                "custom_id": "request-1",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-3.5-turbo",
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": "What is 2+2?"},
                    ],
                },
            },
            {
                "custom_id": "request-2",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-3.5-turbo",
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant."},
                        {
                            "role": "user",
                            "content": "Who won the world series in 2020?",
                        },
                    ],
                },
            },
        ]:
            yield x

    json_iterator_output = upload_jsonl_file(jsonl_in=jsons(), openai_organization="testorg")
    assert json_iterator_output == "file-abc123"

    jsonl_file_output = upload_jsonl_file(jsonl_in="data.jsonl", openai_organization="testorg")
    assert jsonl_file_output == "file-abc123"


file_id_return_values = {
    "output_file_id": b'{"id": "batch_req_zmATcGYZwrxwCNcWO8gjyomw", "custom_id": "request-2", "response": {"status_code": 200, "request_id": "2fb24198ef54a9903851e261bf94fc13", "body": {"id": "chatcmpl-9GoRDkN14rYsLFriDDSke8pRcSnim", "object": "chat.completion", "created": 1713794159, "model": "gpt-3.5-turbo-0125", "choices": [{"index": 0, "message": {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."}, "logprobs": null, "finish_reason": "stop"}], "usage": {"prompt_tokens": 27, "completion_tokens": 13, "total_tokens": 40}, "system_fingerprint": "fp_c2295e73ad"}}, "error": null}{"id": "batch_req_Y1IB4VHLr2vMV9C5qZSIxR2I", "custom_id": "request-1", "response": {"status_code": 200, "request_id": "1ebf9ca826b68e4a842691531a3707c3", "body": {"id": "chatcmpl-9GoRDbG2FQkqbHYKSPbmKuPykCMzp", "object": "chat.completion", "created": 1713794159, "model": "gpt-3.5-turbo-0125", "choices": [{"index": 0, "message": {"role": "assistant", "content": "2 + 2 = 4"}, "logprobs": null, "finish_reason": "stop"}], "usage": {"prompt_tokens": 24, "completion_tokens": 7, "total_tokens": 31}, "system_fingerprint": "fp_c2295e73ad"}}, "error": null}',
    "error_file_id": b"null",
}


def mock_content(file_id):
    return file_id_return_values.get(file_id, "Default return value")


@mock.patch("openai.files.Files.content", side_effect=mock_content)
def test_download_files_task():
    result = download_files(
        batch_endpoint_result='{"output_file_id": "file-output", "error_file_id": "file-error"}',
        openai_organization="testorg",
    )
    assert len(result.keys()) == 1

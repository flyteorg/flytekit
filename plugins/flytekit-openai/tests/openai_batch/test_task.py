import dataclasses
import os
import tempfile
from collections import OrderedDict
from unittest import mock

import jsonlines
from flytekitplugins.openai import (
    BatchEndpointTask,
    DownloadJSONFilesTask,
    OpenAIFileConfig,
    UploadJSONLFileTask,
)
from openai.types import FileObject

from flytekit import Secret
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.extend import get_serializable
from flytekit.models.types import SimpleType
from flytekit.types.file import JSONLFile

JSONL_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data.jsonl")


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
    assert batch_endpoint_task_spec.template.interface.outputs["result"].type.simple == SimpleType.STRUCT


@mock.patch(
    "openai.resources.files.Files.create",
    return_value=FileObject(
        id="file-abc123",
        object="file",
        bytes=120000,
        created_at=1677610602,
        filename="mydata.jsonl",
        purpose="fine-tune",
        status="uploaded",
    ),
)
@mock.patch("flytekit.current_context")
def test_upload_jsonl_files_task(mock_context, mock_file_creation):
    mocked_token = "mocked_openai_api_key"
    mock_context.return_value.secrets.get.return_value = mocked_token
    mock_context.return_value.working_directory = "/tmp"

    upload_jsonl_files_task_obj = UploadJSONLFileTask(
        name="upload-jsonl-1",
        task_config=OpenAIFileConfig(
            openai_organization="testorg",
            secret=Secret(group="test-openai", key="test-key"),
        ),
    )

    jsonl_file_output = upload_jsonl_files_task_obj(jsonl_in=JSONLFile(JSONL_FILE))
    assert jsonl_file_output == "file-abc123"


@mock.patch("openai.resources.files.FilesWithStreamingResponse")
@mock.patch("flytekit.current_context")
@mock.patch("flytekitplugins.openai.batch.task.Path")
def test_download_files_task(mock_path, mock_context, mock_streaming):
    mocked_token = "mocked_openai_api_key"
    mock_context.return_value.secrets.get.return_value = mocked_token

    download_json_files_task_obj = DownloadJSONFilesTask(
        name="download-json-files",
        task_config=OpenAIFileConfig(
            openai_organization="testorg",
            secret=Secret(group="test-openai", key="test-key"),
        ),
    )

    temp_dir = tempfile.TemporaryDirectory()
    temp_file_path = os.path.join(temp_dir.name, "output.jsonl")

    with open(temp_file_path, "w") as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all([{"id": ""}, {"id": ""}])  # dummy outputs

    mock_path.return_value.with_suffix.return_value = temp_file_path

    response_mock = mock.MagicMock()
    mock_streaming.return_value.content.return_value.__enter__.return_value = response_mock
    response_mock.stream_to_file.return_value = None

    output = download_json_files_task_obj(
        batch_endpoint_result={
            "id": "batch_abc123",
            "completion_window": "24h",
            "created_at": 1711471533,
            "endpoint": "/v1/completions",
            "input_file_id": "file-abc123",
            "object": "batch",
            "status": "completed",
            "cancelled_at": None,
            "cancelling_at": None,
            "completed_at": 1711493163,
            "error_file_id": "file-HOWS94",
            "errors": None,
            "expired_at": None,
            "expires_at": 1711557933,
            "failed_at": None,
            "finalizing_at": 1711493133,
            "in_progress_at": 1711471538,
            "metadata": {
                "customer_id": "user_123456789",
                "batch_description": "Nightly eval job",
            },
            "output_file_id": "file-cvaTdG",
            "request_counts": {"completed": 95, "failed": 5, "total": 100},
        }
    )
    assert dataclasses.is_dataclass(output)
    assert output.output_file is not None
    assert output.error_file is not None

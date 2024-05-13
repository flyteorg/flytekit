from typing import Any, Dict, Iterator

from flytekit import Workflow
from flytekit.types.file import JSONLFile
from flytekit.types.iterator import JSON

from .task import (
    BatchEndpointTask,
    BatchResult,
    DownloadJSONFilesTask,
    OpenAIFileConfig,
    UploadJSONLFileTask,
)


def create_batch(
    name: str,
    openai_organization,
    config: Dict[str, Any] = {},
    is_json_iterator: bool = True,
) -> Workflow:
    wf = Workflow(name=f"openai-batch-{name}")

    if is_json_iterator:
        wf.add_workflow_input("json_iterator", Iterator[JSON])
    else:
        wf.add_workflow_input("jsonl_file", JSONLFile)

    upload_jsonl_file_task_obj = UploadJSONLFileTask(
        name=f"openai-file-upload-{name}",
        task_config=OpenAIFileConfig(openai_organization=openai_organization),
    )
    batch_endpoint_task_obj = BatchEndpointTask(
        name=f"openai-batch-{name}",
        openai_organization=openai_organization,
        config=config,
    )
    download_json_files_task_obj = DownloadJSONFilesTask(
        name=f"openai-download-files-{name}",
        task_config=OpenAIFileConfig(openai_organization=openai_organization),
    )

    node_1 = wf.add_entity(
        upload_jsonl_file_task_obj,
        json_iterator=wf.inputs.get("json_iterator"),
        jsonl_file=wf.inputs.get("jsonl_file"),
    )
    node_2 = wf.add_entity(
        batch_endpoint_task_obj,
        input_file_id=node_1.outputs["result"],
    )
    node_3 = wf.add_entity(
        download_json_files_task_obj,
        batch_endpoint_result=node_2.outputs["result"],
    )

    wf.add_workflow_output("batch_output", node_3.outputs["result"], BatchResult)

    return wf

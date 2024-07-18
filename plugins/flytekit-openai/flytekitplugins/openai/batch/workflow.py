from typing import Any, Dict, Iterator, Optional

from flytekit import Resources, Workflow
from flytekit.models.security import Secret
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
    secret: Secret,
    openai_organization: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    is_json_iterator: bool = True,
    file_upload_mem: str = "700Mi",
    file_download_mem: str = "700Mi",
) -> Workflow:
    """
    Uploads JSON data to a JSONL file, creates a batch, waits for it to complete, and downloads the output/error JSON files.

    :param name: The suffix to be added to workflow and task names.
    :param openai_organization: Name of the OpenAI organization.
    :param secret: Secret comprising the OpenAI API key.
    :param config: Additional config for batch creation.
    :param is_json_iterator: Set to True if you're sending an iterator/generator; if a JSONL file, set to False.
    :param file_upload_mem: Memory to allocate to the upload file task.
    :param file_download_mem: Memory to allocate to the download file task.
    """
    wf = Workflow(name=f"openai-batch-{name.replace('.', '')}")

    if is_json_iterator:
        wf.add_workflow_input("jsonl_in", Iterator[JSON])
    else:
        wf.add_workflow_input("jsonl_in", JSONLFile)

    upload_jsonl_file_task_obj = UploadJSONLFileTask(
        name=f"openai-file-upload-{name.replace('.', '')}",
        task_config=OpenAIFileConfig(openai_organization=openai_organization, secret=secret),
    )
    if config is None:
        config = {}
    batch_endpoint_task_obj = BatchEndpointTask(
        name=f"openai-batch-{name.replace('.', '')}",
        openai_organization=openai_organization,
        config=config,
    )
    download_json_files_task_obj = DownloadJSONFilesTask(
        name=f"openai-download-files-{name.replace('.', '')}",
        task_config=OpenAIFileConfig(openai_organization=openai_organization, secret=secret),
    )

    node_1 = wf.add_entity(
        upload_jsonl_file_task_obj,
        jsonl_in=wf.inputs["jsonl_in"],
    )
    node_2 = wf.add_entity(
        batch_endpoint_task_obj,
        input_file_id=node_1.outputs["result"],
    )
    node_3 = wf.add_entity(
        download_json_files_task_obj,
        batch_endpoint_result=node_2.outputs["result"],
    )

    node_1.with_overrides(requests=Resources(mem=file_upload_mem), limits=Resources(mem=file_upload_mem))
    node_3.with_overrides(requests=Resources(mem=file_download_mem), limits=Resources(mem=file_download_mem))

    wf.add_workflow_output("batch_output", node_3.outputs["result"], BatchResult)

    return wf

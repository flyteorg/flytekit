from typing import Any, Dict, Iterator

from flytekit import Workflow
from flytekit.types.file import JSONLFile
from flytekit.types.iterator import JSON

from .task import BatchEndpointTask, BatchResult, download_files, upload_jsonl_file


def create_batch(
    name: str,
    openai_organization,
    config: Dict[str, Any] = {},
    json_iterator: bool = True,
) -> Workflow:
    wf = Workflow(name=f"openai-batch-{name}")

    if json_iterator:
        wf.add_workflow_input("json_iterator", JSONLFile)
    else:
        wf.add_workflow_input("jsonl_file", Iterator[JSON])

    batch_endpoint_task_obj = BatchEndpointTask(
        name=f"openai-batch-{name}",
        openai_organization=openai_organization,
        config=config,
    )

    node_1 = wf.add_entity(
        upload_jsonl_file,
        json_iterator=wf.inputs.get("json_iterator"),
        jsonl_file=wf.inputs.get("jsonl_file"),
        openai_organization=openai_organization,
    )
    node_2 = wf.add_entity(
        batch_endpoint_task_obj,
        input_file_id=node_1.outputs["o0"],
    )
    node_3 = wf.add_entity(
        download_files,
        batch_endpoint_result=node_2.outputs["result"],
        openai_organization=openai_organization,
    )

    wf.add_workflow_output("batch_output", node_3.outputs["o0"], BatchResult)

    return wf

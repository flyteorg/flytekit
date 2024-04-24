from typing import Any, Dict, Iterator

from flytekit import Workflow
from flytekit.types.file import FlyteFile, JSONLFile
from flytekit.types.iterator import JSON

from .task import BatchEndpointTask, download_files, upload_jsonl_file


def create_batch(
    name: str,
    openai_organization,
    config: Dict[str, Any] = {},
    connection: str = "",
) -> Workflow:
    wf = Workflow(name=f"openai-batch-endpoint-{name}")
    wf.add_workflow_input("jsonl_in", JSONLFile | Iterator[JSON])

    batch_endpoint_task_obj = BatchEndpointTask(
        name=f"openai-batch-endpoint-{name}",
        openai_organization=openai_organization,
        config=config,
        connection=connection,
    )

    node_1 = wf.add_entity(
        upload_jsonl_file,
        jsonl_in=wf.inputs["jsonl_in"],
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

    wf.add_workflow_output("batch_output", node_3.outputs["o0"], dict[str, FlyteFile])

    return wf

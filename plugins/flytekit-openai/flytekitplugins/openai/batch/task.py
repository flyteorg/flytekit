import json
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import jsonlines

import flytekit
from flytekit import Resources, kwtypes, lazy_module, task
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.models.security import Secret, SecurityContext
from flytekit.types.file import JSONLFile
from flytekit.types.iterator import JSON

openai = lazy_module("openai")


class BatchEndpointTask(AsyncAgentExecutorMixin, PythonTask):
    _TASK_TYPE = "openai-batch-endpoint"

    def __init__(
        self,
        name: str,
        openai_organization: str,
        config: Dict[str, Any] = {},
        connection: str = "openai-batch-endpoint",
        **kwargs,
    ):
        sec_ctx = SecurityContext(connection=connection)

        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            interface=Interface(
                inputs=kwtypes(input_file_id=str),
                outputs=kwtypes(result=Optional[str]),
            ),
            security_ctx=sec_ctx,
            **kwargs,
        )

        self._openai_organization = openai_organization
        self._config = config

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self._openai_organization,
            "config": self._config,
        }


OPENAI_API_KEY = "api-key"


@task(
    secret_requests=Secret(
        key=OPENAI_API_KEY,
        mount_requirement=Secret.MountType.FILE,
    ),
    requests=Resources(mem="700Mi"),
)
def upload_jsonl_file(jsonl_in: JSONLFile | Iterator[JSON], openai_organization: str) -> str:
    client = openai.OpenAI(
        organization=openai_organization,
        api_key=flytekit.current_context().secrets.get(key=OPENAI_API_KEY),
    )

    if isinstance(jsonl_in, JSONLFile):
        local_jsonl_file = jsonl_in.download()
    elif isinstance(jsonl_in, Iterator):
        local_jsonl_file = Path(flytekit.current_context().working_directory, "local.jsonl").as_posix()
        with open(local_jsonl_file, "w") as w:
            with jsonlines.Writer(w) as writer:
                for json_val in jsonl_in:
                    writer.write(json_val)

    # The file can be a maximum of 512 MB
    uploaded_file_obj = client.files.create(file=open(local_jsonl_file, "rb"), purpose="batch")
    return uploaded_file_obj.id


@task(
    secret_requests=Secret(
        key=OPENAI_API_KEY,
        mount_requirement=Secret.MountType.FILE,
    ),
    requests=Resources(mem="700Mi"),
)
def download_files(
    batch_endpoint_result: str,
    openai_organization: str,
) -> dict[str, JSONLFile]:
    client = openai.OpenAI(
        organization=openai_organization,
        api_key=flytekit.current_context().secrets.get(key=OPENAI_API_KEY),
    )

    result = {}
    working_dir = flytekit.current_context().working_directory
    batch_endpoint_result_dict = json.loads(batch_endpoint_result)

    for file_name, file_id in zip(
        ("output_file", "error_file"),
        (
            batch_endpoint_result_dict["output_file_id"],
            batch_endpoint_result_dict["error_file_id"],
        ),
    ):
        if file_id:
            file_content = client.files.content(file_id)

            file_path = Path(working_dir, file_name).with_suffix(".jsonl").as_posix()
            file_content.stream_to_file(file_path)

            result[file_name] = file_path

    return result

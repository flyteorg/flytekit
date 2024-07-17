from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from mashumaro.mixins.json import DataClassJSONMixin

import flytekit
from flytekit import Resources, kwtypes, lazy_module
from flytekit.configuration import SerializationSettings
from flytekit.configuration.default_images import DefaultImages, PythonVersion
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.core.python_customized_container_task import PythonCustomizedContainerTask
from flytekit.core.shim_task import ShimTaskExecutor
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.models.security import Secret
from flytekit.models.task import TaskTemplate
from flytekit.types.file import JSONLFile

openai = lazy_module("openai")


@dataclass
class BatchResult(DataClassJSONMixin):
    output_file: Optional[JSONLFile] = None
    error_file: Optional[JSONLFile] = None


class BatchEndpointTask(AsyncAgentExecutorMixin, PythonTask):
    _TASK_TYPE = "openai-batch"

    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        openai_organization: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            interface=Interface(
                inputs=kwtypes(input_file_id=str),
                outputs=kwtypes(result=Dict),
            ),
            **kwargs,
        )

        self._openai_organization = openai_organization
        self._config = config

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self._openai_organization,
            "config": self._config,
        }


class OpenAIFileDefaultImages(DefaultImages):
    """Default images for the openai batch plugin."""

    _DEFAULT_IMAGE_PREFIXES = {
        PythonVersion.PYTHON_3_8: "cr.flyte.org/flyteorg/flytekit:py3.8-openai-batch-",
        PythonVersion.PYTHON_3_9: "cr.flyte.org/flyteorg/flytekit:py3.9-openai-batch-",
        PythonVersion.PYTHON_3_10: "cr.flyte.org/flyteorg/flytekit:py3.10-openai-batch-",
        PythonVersion.PYTHON_3_11: "cr.flyte.org/flyteorg/flytekit:py3.11-openai-batch-",
        PythonVersion.PYTHON_3_12: "cr.flyte.org/flyteorg/flytekit:py3.12-openai-batch-",
    }


@dataclass
class OpenAIFileConfig:
    secret: Secret
    openai_organization: Optional[str] = None

    def _secret_to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "group": self.secret.group,
            "key": self.secret.key,
            "group_version": self.secret.group_version,
            "mount_requirement": self.secret.mount_requirement.value,
        }


class UploadJSONLFileTask(PythonCustomizedContainerTask[OpenAIFileConfig]):
    _UPLOAD_JSONL_FILE_TASK_TYPE = "openai-batch-upload-file"

    def __init__(
        self,
        name: str,
        task_config: OpenAIFileConfig,
        container_image: str = OpenAIFileDefaultImages.find_image_for(),
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._UPLOAD_JSONL_FILE_TASK_TYPE,
            executor_type=UploadJSONLFileExecutor,
            container_image=container_image,
            requests=Resources(mem="700Mi"),
            interface=Interface(
                inputs=kwtypes(
                    jsonl_in=JSONLFile,
                ),
                outputs=kwtypes(result=str),
            ),
            secret_requests=[task_config.secret],
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self.task_config.openai_organization,
            "secret_arg": self.task_config._secret_to_dict(),
        }


class UploadJSONLFileExecutor(ShimTaskExecutor[UploadJSONLFileTask]):
    def execute_from_model(self, tt: TaskTemplate, **kwargs) -> Any:
        secret = tt.custom["secret_arg"]
        client = openai.OpenAI(
            organization=tt.custom["openai_organization"],
            api_key=flytekit.current_context().secrets.get(
                group=secret["group"],
                key=secret["key"],
                group_version=secret["group_version"],
            ),
        )

        local_jsonl_file = kwargs["jsonl_in"].download()
        uploaded_file_obj = client.files.create(file=open(local_jsonl_file, "rb"), purpose="batch")
        return uploaded_file_obj.id


class DownloadJSONFilesTask(PythonCustomizedContainerTask[OpenAIFileConfig]):
    _DOWNLOAD_JSON_FILES_TASK_TYPE = "openai-batch-download-files"

    def __init__(
        self,
        name: str,
        task_config: OpenAIFileConfig,
        container_image: str = OpenAIFileDefaultImages.find_image_for(),
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._DOWNLOAD_JSON_FILES_TASK_TYPE,
            executor_type=DownloadJSONFilesExecutor,
            container_image=container_image,
            requests=Resources(mem="700Mi"),
            interface=Interface(
                inputs=kwtypes(batch_endpoint_result=Dict),
                outputs=kwtypes(result=BatchResult),
            ),
            secret_requests=[task_config.secret],
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self.task_config.openai_organization,
            "secret_arg": self.task_config._secret_to_dict(),
        }


class DownloadJSONFilesExecutor(ShimTaskExecutor[DownloadJSONFilesTask]):
    def execute_from_model(self, tt: TaskTemplate, **kwargs) -> Any:
        secret = tt.custom["secret_arg"]
        client = openai.OpenAI(
            organization=tt.custom["openai_organization"],
            api_key=flytekit.current_context().secrets.get(
                group=secret["group"],
                key=secret["key"],
                group_version=secret["group_version"],
            ),
        )

        batch_result = BatchResult()
        working_dir = flytekit.current_context().working_directory

        for file_name, file_id in zip(
            ("output_file", "error_file"),
            (
                kwargs["batch_endpoint_result"]["output_file_id"],
                kwargs["batch_endpoint_result"]["error_file_id"],
            ),
        ):
            if file_id:
                file_path = str(Path(working_dir, file_name).with_suffix(".jsonl"))

                with client.files.with_streaming_response.content(file_id) as response:
                    response.stream_to_file(file_path)

                setattr(batch_result, file_name, JSONLFile(file_path))

        return batch_result

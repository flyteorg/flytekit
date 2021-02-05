from enum import Enum
from typing import Any, Dict, List, Optional, Type

from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.context_manager import SerializationSettings
from flytekit.core.interface import Interface
from flytekit.models import task as _task_model


class ContainerTask(PythonTask):
    class MetadataFormat(Enum):
        JSON = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_JSON
        YAML = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_YAML
        PROTO = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_PROTO

    class IOStrategy(Enum):
        DOWNLOAD_EAGER = _task_model.IOStrategy.DOWNLOAD_MODE_EAGER
        DOWNLOAD_STREAM = _task_model.IOStrategy.DOWNLOAD_MODE_STREAM
        DO_NOT_DOWNLOAD = _task_model.IOStrategy.DOWNLOAD_MODE_NO_DOWNLOAD
        UPLOAD_EAGER = _task_model.IOStrategy.UPLOAD_MODE_EAGER
        UPLOAD_ON_EXIT = _task_model.IOStrategy.UPLOAD_MODE_ON_EXIT
        DO_NOT_UPLOAD = _task_model.IOStrategy.UPLOAD_MODE_NO_UPLOAD

    def __init__(
        self,
        name: str,
        image: str,
        command: List[str],
        inputs: Optional[Dict[str, Type]] = None,
        metadata: Optional[TaskMetadata] = None,
        arguments: List[str] = None,
        outputs: Dict[str, Type] = None,
        input_data_dir: str = None,
        output_data_dir: str = None,
        metadata_format: MetadataFormat = MetadataFormat.JSON,
        io_strategy: IOStrategy = None,
        **kwargs,
    ):
        super().__init__(
            task_type="raw-container",
            name=name,
            interface=Interface(inputs, outputs),
            metadata=metadata,
            task_config=None,
            **kwargs,
        )
        self._image = image
        self._cmd = command
        self._args = arguments
        self._input_data_dir = input_data_dir
        self._output_data_dir = output_data_dir
        self._md_format = metadata_format
        self._io_strategy = io_strategy

    def execute(self, **kwargs) -> Any:
        print(kwargs)
        print(
            f"\ndocker run --rm -v /tmp/inputs:{self._input_data_dir} -v /tmp/outputs:{self._output_data_dir} -e x=y "
            f"{self._image} {self._cmd} {self._args}"
        )
        return None

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=self._image,
            command=self._cmd,
            args=self._args,
            data_loading_config=_task_model.DataLoadingConfig(
                input_path=self._input_data_dir,
                output_path=self._output_data_dir,
                format=self._md_format.value,
                enabled=True,
                io_strategy=self._io_strategy.value if self._io_strategy else None,
            ),
            environment=env,
        )

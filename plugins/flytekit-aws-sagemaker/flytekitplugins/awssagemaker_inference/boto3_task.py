from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, Union

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import SyncAgentExecutorMixin
from flytekit.image_spec.image_spec import ImageBuildEngine


@dataclass
class BotoConfig(object):
    service: str
    method: str
    config: Dict[str, Any]
    region: Optional[str] = None
    images: Optional[Dict[str, Union[str, ImageSpec]]] = None


class BotoTask(SyncAgentExecutorMixin, PythonTask[BotoConfig]):
    _TASK_TYPE = "boto"

    def __init__(
        self,
        name: str,
        task_config: BotoConfig,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            interface=Interface(
                inputs=inputs,
                outputs=kwtypes(result=dict, idempotence_token=str),
            ),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        images = self.task_config.images
        if images is not None:
            for key, image in images.items():
                if isinstance(image, ImageSpec):
                    # Build the image
                    ImageBuildEngine.build(image)

                    # Replace the value in the dictionary with image.image_name()
                    images[key] = image.image_name()

        return {
            "service": self.task_config.service,
            "config": self.task_config.config,
            "region": self.task_config.region,
            "method": self.task_config.method,
            "images": images,
        }

from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, Optional, Type, Union

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import SyncAgentExecutorMixin
from flytekit.image_spec.image_spec import ImageBuildEngine

account_id_map = {
    "us-east-1": "785573368785",
    "us-east-2": "007439368137",
    "us-west-1": "710691900526",
    "us-west-2": "301217895009",
    "eu-west-1": "802834080501",
    "eu-west-2": "205493899709",
    "eu-west-3": "254080097072",
    "eu-north-1": "601324751636",
    "eu-south-1": "966458181534",
    "eu-central-1": "746233611703",
    "ap-east-1": "110948597952",
    "ap-south-1": "763008648453",
    "ap-northeast-1": "941853720454",
    "ap-northeast-2": "151534178276",
    "ap-southeast-1": "324986816169",
    "ap-southeast-2": "355873309152",
    "cn-northwest-1": "474822919863",
    "cn-north-1": "472730292857",
    "sa-east-1": "756306329178",
    "ca-central-1": "464438896020",
    "me-south-1": "836785723513",
    "af-south-1": "774647643957",
}


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
            interface=Interface(inputs=inputs, outputs=kwtypes(result=dict)),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        region = self.task_config.region

        images = self.task_config.images
        base = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"

        if images is not None:
            for image_name, image in images.items():
                if isinstance(image, ImageSpec):
                    # Build the image
                    ImageBuildEngine.build(image)

                    # Replace the value in the dictionary with image.image_name()
                    images[image_name] = image.image_name()
                elif isinstance(image, partial):
                    images[image_name] = image(
                        account_id=account_id_map[region],
                        region=region,
                        base=base,
                    )

        return {
            "service": self.task_config.service,
            "config": self.task_config.config,
            "region": self.task_config.region,
            "method": self.task_config.method,
            "images": images,
        }

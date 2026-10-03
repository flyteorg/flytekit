import asyncio
import re
import subprocess
import argparse
from typing import Optional
from dataclasses import dataclass
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta

@dataclass
class RunpodMetadata(ResourceMeta):
    pod_id: str

class RunpodAgent(AsyncAgentBase):
    def __init__(self):
        super().__init__(task_type_name="Runpod", metadata_type=RunpodMetadata)

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> RunpodMetadata:
        image_name = task_template.container.image
        output = subprocess.check_output(f'runpodctl create pod --gpuType "NVIDIA GeForce RTX 3070" --imageName "{image_name}"', shell=True)
        pattern = r'pod "([^"]+)"'
        match = re.search(pattern, output.decode('utf-8'))
        if match:
            pod_id = match.group(1)
            print(f'The extracted pod ID is: {pod_id}')
            return RunpodMetadata(pod_id=pod_id)
        else:
            print('Pod ID not found in the string.')
            raise ValueError(f"Pod Id not found {output}")

    async def get(self, resource_meta: RunpodMetadata, **kwargs) -> Resource:
        output = subprocess.check_output(f'runpodctl get pod {resource_meta.pod_id}', shell = True)
        lines = output.strip().split('\n')
        if len(lines) > 1:
            pod_info = lines[1]
            status = pod_info.split('\t')[-1]
            print(f"The status  of the pod is {output}")

        return resource_meta.output
        
    async def delete(self, resource_meta: RunpodMetadata, **kwargs):
        subprocess.Popen(f'runpodctl stop pod {resource_meta.pod_id}', shell=True)
        subprocess.Popen(f'runpodctl remove pod {resource_meta.pod_id}', shell=True)

AgentRegistry.register(RunpodAgent())

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
        #image_name = task_template.container.image
        image_name = "my image"
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
                    #subprocess.Popen(f'runpodctl create pod', shell=True)

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


parser = argparse.ArgumentParser(description='Runpod Agent')

create_parser = parser.add_subparsers(dest='command', help='Command to execute')

apiKeyConfig_command = create_parser.add_parser('apikeyconfig', help='Config an API Key')
apiKeyConfig_command.add_argument('--api_key', type=str, required=True, help='Your API Key')

create_command = create_parser.add_parser('create', help='Create a pod')
create_command.add_argument('--gpu_type', type=str, required=False, help='The type of GPU')
create_command.add_argument('--image_name', type=str, required=False, help='The name of the image')

get_command = create_parser.add_parser('get', help='Get pod information')
get_command.add_argument('--pod_id', type=str, required=False, help='The ID of the pod')

delete_command = create_parser.add_parser('delete', help='Delete a pod')
delete_command.add_argument('--pod_id', type=str, required=True, help='The ID of the pod')

args = parser.parse_args()

sawsan = RunpodAgent()

if args.command == 'create':
    asyncio.run(sawsan.create(args.gpu_type, args.image_name))
elif args.command == 'get':
    sawsan.get(args.pod_id)
elif args.command == 'delete':
    sawsan.delete(args.pod_id)
elif args.command == 'createTwo':
    sawsan.createTwo()
else:
    parser.print_help()

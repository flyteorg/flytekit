import subprocess
import argparse
from typing import Optional
from dataclasses import dataclass
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
class RunpodMetadata(ResourceMeta):

    pod_id: str
class RunpodAgent(AsyncAgentBase):
    def __init__(self):
        super().__init__(task_type_name="Runpod", metadata_type=RunpodMetadata)

    def apiKeyConfig(self, api_key):
        subprocess.Popen(f'runpodctl config --apiKey {api_key}', shell=True)

    def createTwo(self):
        
        subprocess.Popen(f'runpodctl create pod --gpuType "NVIDIA GeForce RTX 3070" --imageName "image_name"', shell=True)
        #subprocess.Popen(f'runpodctl create pod', shell=True)
    def create(self, gpu_type, image_name):
        
        subprocess.Popen(f'runpodctl create pod --gpuType "{gpu_type}" --imageName "{image_name}"', shell=True)
        #subprocess.Popen(f'runpodctl create pod', shell=True)

    def get(self, pod_id):
        subprocess.Popen(f'runpodctl get pod {pod_id}', shell=True)

    def delete(self, pod_id):
        subprocess.Popen(f'runpodctl stop pod {pod_id}', shell=True)
        subprocess.Popen(f'runpodctl remove pod {pod_id}', shell=True)

parser = argparse.ArgumentParser(description='Runpod Agent')

create_parser = parser.add_subparsers(dest='command', help='Command to execute')

apiKeyConfig_command = create_parser.add_parser('apikeyconfig', help='Config an API Key')
apiKeyConfig_command.add_argument('--api_key', type=str, required=True, help='Your API Key')

create_command = create_parser.add_parser('create', help='Create a pod')
create_command.add_argument('--gpu_type', type=str, required=True, help='The type of GPU')
create_command.add_argument('--image_name', type=str, required=True, help='The name of the image')

createTwo_command = create_parser.add_parser('createTwo', help='Create a pod')

get_command = create_parser.add_parser('get', help='Get pod information')
get_command.add_argument('--pod_id', type=str, required=False, help='The ID of the pod')

delete_command = create_parser.add_parser('delete', help='Delete a pod')
delete_command.add_argument('--pod_id', type=str, required=True, help='The ID of the pod')

args = parser.parse_args()

sawsan = RunpodAgent()

if args.command == 'create':
    sawsan.create(args.gpu_type, args.image_name)
elif args.command == 'get':
    sawsan.get(args.pod_id)
elif args.command == 'delete':
    sawsan.delete(args.pod_id)
elif args.command == 'createTwo':
    sawsan.createTwo()
else:
    parser.print_help()

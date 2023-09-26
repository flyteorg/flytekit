from dataclasses import dataclass
from typing import Optional

import cloudpickle
import grpc
from flyteidl.admin.agent_pb2 import CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource
import boto3
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class Metadata:
    job_id: str


class AWSBatchAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="aws-batch", asynchronous=False)

    def _get_client(self):
        """
        Get a boto3 client for AWS Batch
        :rtype: boto3.client
        """
        return boto3.client('batch', region_name='us-west-2')

    def create(
            self,
            context: grpc.ServicerContext,
            output_prefix: str,
            task_template: TaskTemplate,
            inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        client = self._get_client()
        resources = task_template.container.resources
        config = task_template.custom
        print(resources.requests)
        print(task_template.container.args)
        container_properties = {
            'image': task_template.container.image,
            'vcpus': 1,
            'memory': 512,
            'environment': [task_template.container.env],
            'command': task_template.container.args,
            'jobRoleArn': 'arn:aws:iam::590375264460:role/ecsTaskExecutionRole'
            # Convert Resource string to integer
            # retry
            # jobRoleArn
            # ... other container properties ...
        }
        response = client.register_job_definition(jobDefinitionName="flyte-batch",
                                                  type="container",
                                                  containerProperties=container_properties,
                                                  platformCapabilities=[config['platformCapabilities']],
                                                  timeout={'attemptDurationSeconds': task_template.metadata.timeout or 3600*24},
                                                  )
        response = client.submit_job(jobName="test", jobQueue="flyte-test", jobDefinition=response['jobDefinitionName'])
        return CreateTaskResponse(resource_meta=cloudpickle.dumps(response["jobId"]))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        client = self._get_client()
        job_id = cloudpickle.loads(resource_meta)
        response = client.describe_jobs(jobs=[job_id])
        status = response['jobs'][0]['status']
        cur_state = convert_to_flyte_state(status)
        return GetTaskResponse(resource=Resource(state=cur_state))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        client = self._get_client()
        job_id = cloudpickle.loads(resource_meta)
        client.terminate_job(jobId=job_id, reason='Cancelling job.')
        return DeleteTaskResponse()


AgentRegistry.register(AWSBatchAgent())

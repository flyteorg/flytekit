from dataclasses import dataclass
from typing import Optional

import cloudpickle
import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource

from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class Metadata:
    job_id: str


class MemvergeAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="memverge_task")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        # print("task template", task_template)
        print("container args", task_template.container.args)
        # container args ['pyflyte-fast-execute', '--additional-distribution',
        # 's3://my-s3-bucket/flytesnacks/development/XYP3YCR5RKERZXBH5ACD7E56PY======/script_mode.tar.gz',
        # '--dest-dir', '/root', '--', 'pyflyte-execute', '--inputs',
        # 's3://my-s3-bucket/metadata/propeller/flytesnacks-development-axq2rsxnmht57kq9sxpt/n0/data/inputs.pb',
        # '--output-prefix', 's3://my-s3-bucket/metadata/propeller/flytesnacks-development-axq2rsxnmht57kq9sxpt/n0/data/0',
        # '--raw-output-data-prefix', 's3://my-s3-bucket/test/vr/axq2rsxnmht57kq9sxpt-n0-0', '--checkpoint-path',
        # 's3://my-s3-bucket/test/vr/axq2rsxnmht57kq9sxpt-n0-0/_flytecheckpoints', '--prev-checkpoint', '""',
        # '--resolver', 'flytekit.core.python_auto_container.default_task_resolver', '--', 'task-module',
        # 'memverge_wf', 'task-name', 't1']
        print("container image", task_template.container.image)
        # container image pingsutw/flytekit:GAC0mevQ3yV7P0Q8uH6JwQ..
        job_id = "memverge_job_id"
        return CreateTaskResponse(resource_meta=cloudpickle.dumps(job_id))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        # Get job status
        job_id = cloudpickle.loads(resource_meta)
        # float get status --id job_id
        return GetTaskResponse(resource=Resource(state=SUCCEEDED))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(MemvergeAgent())

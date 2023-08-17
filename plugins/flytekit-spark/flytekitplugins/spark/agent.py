import json
from dataclasses import asdict, dataclass
from typing import Optional

import grpc
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.compute import DockerBasicAuth, DockerImage
from flyteidl.admin.agent_pb2 import (
    PENDING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)

from flytekit import FlyteContextManager, StructuredDataset, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType


@dataclass
class Metadata:
    cluster_id: str
    host: str
    token: str
    run_id: int


class DatabricksAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="spark", asynchronous=False)

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        for attr_name, attr_value in vars(task_template).items():
            print(f"Type:{type(attr_value)}, {attr_name}: {attr_value}")

        custom = task_template.custom
        for attr_name, attr_value in custom.items():
            print(f"{attr_name}: {attr_value}")

        w = WorkspaceClient(host=custom["host"], token=custom["token"])
        # to be done, docker image, azure, aws, gcp
        docker_image_conf = custom["docker_image_conf"]
        basic_auth_conf = docker_image_conf.get("basic_auth", {})
        auth = DockerBasicAuth(
            username=basic_auth_conf.get("username"),
            password=basic_auth_conf.get("password"),
        )
        docker_image = DockerImage(
            url=docker_image_conf.get("url"),
            basic_auth=auth,
        )

        clstr = w.clusters.create_and_wait(
            cluster_name=custom["cluster_name"],
            docker_image=docker_image,
            spark_version=custom["spark_version"],
            node_type_id=custom["node_type_id"],
            autotermination_minutes=custom["autotermination_minutes"],
            num_workers=custom["num_workers"],
        )
        cluster_id = clstr.cluster_id  # important metadata

        tasks = [
            jobs.Task(
                description=custom["description"],
                existing_cluster_id=cluster_id,
                spark_python_task=jobs.SparkPythonTask(python_file=custom["python_file"]),
                task_key=custom["task_key"],  # metadata
                timeout_seconds=custom["timeout_seconds"],  # metadata
            )
        ]

        run = w.jobs.submit(
            name=custom["cluster_name"],  # metadata
            tasks=tasks,  # tasks
        ).result()

        # metadata
        metadata = Metadata(
            cluster_id=cluster_id,
            host=custom["host"],
            token=custom["token"],
            run_id=run.tasks[0].run_id,
        )

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        w = WorkspaceClient(
            host=metadata.host,
            token=metadata.token,
        )
        job = w.jobs.get_run_output(metadata.run_id)

        if job.error:  # have already checked databricks sdk
            logger.error(job.errors.__str__())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(job.errors.__str__())
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))

        if job.metadata.state.result_state == jobs.RunResultState.SUCCESS:
            cur_state = SUCCEEDED
        else:
            # TODO: Discuss with Kevin for the state, considering mapping technique
            cur_state = PENDING

        res = None

        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            # output page_url and task_output
            if job.metadata.run_page_url:
                output_location = job.metadata.run_page_url
                res = literals.LiteralMap(
                    {
                        "results": TypeEngine.to_literal(
                            ctx,
                            StructuredDataset(uri=output_location),
                            StructuredDataset,
                            LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                        )
                    }
                ).to_flyte_idl()
            w.clusters.permanent_delete(cluster_id=metadata.cluster_id)

        return GetTaskResponse(resource=Resource(state=cur_state, outputs=res))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        w = WorkspaceClient(
            host=metadata.host,
            token=metadata.token,
        )
        w.jobs.delete_run(metadata.run_id)
        w.clusters.permanent_delete(cluster_id=metadata.cluster_id)
        return DeleteTaskResponse()


AgentRegistry.register(DatabricksAgent())

from random import randint

from flyteidl.service import plugin_system_pb2

from flytekit import FlyteContextManager, StructuredDataset
from flytekit.core import constants
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry
from flytekit.extend.backend.utils import upload_output_file
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType


# This plugin is used for performance benchmarking
class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    def initialize(self):
        pass

    def create(
        self, inputs: LiteralMap, output_prefix: str, task_template: TaskTemplate
    ) -> plugin_system_pb2.TaskCreateResponse:
        return plugin_system_pb2.TaskCreateResponse(job_id="fake_id")

    def get(
        self, job_id: str, output_prefix: str, prev_state: plugin_system_pb2.State
    ) -> plugin_system_pb2.TaskGetResponse:
        if prev_state == plugin_system_pb2.SUCCEEDED:
            return plugin_system_pb2.TaskGetResponse(state=plugin_system_pb2.SUCCEEDED)

        x = randint(1, 100)
        if x > 50:
            ctx = FlyteContextManager.current_context()
            output_file_dict = {
                constants.OUTPUT_FILE_NAME: literals.LiteralMap(
                    {
                        "results": TypeEngine.to_literal(
                            ctx,
                            StructuredDataset(uri="fake_uri"),
                            StructuredDataset,
                            LiteralType(structured_dataset_type=StructuredDatasetType(format="")),
                        )
                    }
                )
            }
            upload_output_file(output_file_dict, output_prefix)
            state = plugin_system_pb2.SUCCEEDED
        else:
            state = plugin_system_pb2.RUNNING

        return plugin_system_pb2.TaskGetResponse(state=state)

    def delete(self, job_id) -> plugin_system_pb2.TaskDeleteResponse:
        print("deleting")
        return plugin_system_pb2.TaskDeleteResponse()


BackendPluginRegistry.register(DummyPlugin())

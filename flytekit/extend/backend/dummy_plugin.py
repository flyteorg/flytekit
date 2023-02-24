from random import randint

from flyteidl.service import plugin_system_pb2

from flytekit import FlyteContextManager, StructuredDataset
from flytekit.core import constants
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_plugin import (
    BackendPluginBase,
    BackendPluginRegistry,
    CreateRequest,
    CreateResponse,
    PollRequest,
    PollResponse,
)
from flytekit.extend.backend.utils import get_task_inputs, get_task_template, upload_output_file
from flytekit.models import literals
from flytekit.models.types import LiteralType, StructuredDatasetType


# This plugin is used for performance benchmarking
class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    def initialize(self):
        pass

    def create(self, create_request: CreateRequest) -> CreateResponse:
        print("creating")
        return CreateResponse(job_id="fake_id")

    def poll(self, poll_request: PollRequest) -> PollResponse:
        if poll_request.prev_state == plugin_system_pb2.SUCCEEDED:
            return PollResponse(state=plugin_system_pb2.SUCCEEDED)

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
            upload_output_file(output_file_dict, poll_request.output_prefix)
            state = plugin_system_pb2.SUCCEEDED
        else:
            state = plugin_system_pb2.RUNNING

        return PollResponse(state=state)

    def terminate(self, job_id):
        print("deleting")


BackendPluginRegistry.register(DummyPlugin())
